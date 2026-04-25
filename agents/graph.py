from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, TypedDict, Union

from langgraph.graph import END, START, StateGraph

import core
from generation_db import GenerationRepository


class ChunkState(TypedDict, total=False):
    chunk_text: str
    source_name: str
    chunk_idx: int
    total_chunks: int
    doc_title: str
    max_pairs: int
    produced_so_far: int
    skip_review: bool
    prefilter_passed: bool
    prefilter_reason: str
    candidate_pairs: List[Dict]
    accepted_pairs: List[Dict]


def _build_chunk_graph() -> StateGraph:
    graph = StateGraph(ChunkState)

    def prefilter_node(state: ChunkState) -> ChunkState:
        if state.get("skip_review", True):
            return {"prefilter_passed": True, "prefilter_reason": "skipped"}
        accepted, reason = core.prefilter_chunk(state["chunk_text"])
        return {"prefilter_passed": accepted, "prefilter_reason": reason}

    def route_prefilter(state: ChunkState) -> str:
        return "generate" if state.get("prefilter_passed", True) else "finish"

    def generate_node(state: ChunkState) -> ChunkState:
        remaining_budget = max(0, int(state["max_pairs"]) - int(state.get("produced_so_far", 0)))
        remaining_after_this = max(1, int(state["total_chunks"]) - int(state["chunk_idx"]) + 1)
        cap_this_chunk = min(20, max(0, round(remaining_budget / remaining_after_this)))
        candidate_pairs = core.generate_pairs_for_chunk(
            state["chunk_text"],
            state["source_name"],
            title=state["doc_title"],
            cap_this_chunk=cap_this_chunk,
            total_target=state["max_pairs"],
            produced_so_far=state.get("produced_so_far", 0),
            remaining_chunks=remaining_after_this - 1,
            chunk_idx=state["chunk_idx"],
        )
        return {"candidate_pairs": candidate_pairs}

    def review_node(state: ChunkState) -> ChunkState:
        accepted_pairs: List[Dict] = []
        for pair in state.get("candidate_pairs", []):
            question_text = pair.get("question", "")
            answer_text = pair.get("answer", "")
            if core.has_document_reference(question_text) or core.has_document_reference(answer_text):
                continue

            if state.get("skip_review", True):
                q_lower = question_text.lower()
                a_lower = answer_text.lower()
                metadata_keywords = ["file://", "path://", "http://", "https://", "metadata:", "e-mel:", "@", ".com"]
                if any(keyword in q_lower or keyword in a_lower for keyword in metadata_keywords):
                    continue
                reviewed = pair
            else:
                reviewed, _reason = core.review_pair(pair, state["chunk_text"], title=state["doc_title"])
                if not reviewed:
                    continue

            if reviewed and reviewed.get("question") and reviewed.get("answer"):
                accepted_pairs.append(reviewed)
        return {"accepted_pairs": accepted_pairs}

    def finish_node(_state: ChunkState) -> ChunkState:
        return {}

    graph.add_node("prefilter", prefilter_node)
    graph.add_node("generate", generate_node)
    graph.add_node("review", review_node)
    graph.add_node("finish", finish_node)

    graph.add_edge(START, "prefilter")
    graph.add_conditional_edges("prefilter", route_prefilter, {"generate": "generate", "finish": "finish"})
    graph.add_edge("generate", "review")
    graph.add_edge("review", "finish")
    graph.add_edge("finish", END)
    return graph.compile()


def _adaptive_max_pairs(text_content: str, total_chunks: int, user_max_pairs: Optional[int]) -> int:
    word_count = len(text_content.split())
    estimated_pairs = min(word_count // 40, total_chunks * 20)
    adaptive_max = max(50, min(200, estimated_pairs))
    adaptive_max = round(adaptive_max / 10) * 10
    if user_max_pairs is not None and user_max_pairs > 0:
        return min(user_max_pairs, adaptive_max)
    return adaptive_max


def run_generation(
    repo: GenerationRepository,
    generation_id: str,
    text_content: str,
    source_name: str,
    *,
    max_pairs: Optional[int] = None,
    progress_callback: Optional[Callable[[Union[str, Dict[str, Any]]], None]] = None,
    skip_review: bool = True,
    doc_title: Optional[str] = None,
    max_workers: Optional[int] = None,
) -> List[Dict]:
    chunk_graph = _build_chunk_graph()
    chunks = core.chunk_words(text_content, core.CHUNK_WORDS, core.CHUNK_OVERLAP)
    total_chunks = len(chunks)
    if total_chunks == 0:
        return []

    final_max_pairs = _adaptive_max_pairs(text_content, total_chunks, max_pairs)
    workers = max_workers or int(os.getenv("QNA_MAX_WORKERS", "10"))
    workers = max(1, workers)

    shared_lock = Lock()
    accepted_pairs: List[Dict] = []
    existing_questions = repo.list_questions(generation_id)
    for q in existing_questions:
        accepted_pairs.append({"question": q, "answer": "", "source": "", "chunk_text": ""})

    if progress_callback:
        progress_callback(
            {
                "message": (
                    f"Found {total_chunks} chunks. Target: {final_max_pairs} pairs. Processing in parallel..."
                ),
                "chunks_completed": 0,
                "chunks_total": total_chunks,
                "pairs_accepted": len(existing_questions),
                "pairs_target": final_max_pairs,
            }
        )

    chunk_data_list = [(chunk_text, idx, total_chunks) for idx, (chunk_text, _start, _end) in enumerate(chunks, 1)]

    def process_chunk(chunk_data: tuple[str, int, int]) -> List[Dict]:
        chunk_text, idx, total = chunk_data
        with shared_lock:
            produced_so_far = len(existing_questions)
        state: ChunkState = {
            "chunk_text": chunk_text,
            "source_name": source_name,
            "chunk_idx": idx,
            "total_chunks": total,
            "doc_title": (doc_title or source_name),
            "max_pairs": final_max_pairs,
            "produced_so_far": produced_so_far,
            "skip_review": skip_review,
        }
        result = chunk_graph.invoke(state)
        return result.get("accepted_pairs", [])

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_chunk = {
            executor.submit(process_chunk, chunk_data): chunk_data[1]
            for chunk_data in chunk_data_list
        }
        for future in as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            completed += 1
            try:
                chunk_results = future.result()
            except Exception as e:
                msg = f"Error processing chunk {chunk_idx}: {str(e)}"
                repo.add_event(generation_id, "chunk_error", msg)
                if progress_callback:
                    with shared_lock:
                        pairs_now = len(existing_questions)
                    progress_callback(
                        {
                            "message": msg,
                            "chunks_completed": completed,
                            "chunks_total": total_chunks,
                            "pairs_accepted": pairs_now,
                            "pairs_target": final_max_pairs,
                        }
                    )
                continue

            with shared_lock:
                for pair in chunk_results:
                    if len(existing_questions) >= final_max_pairs:
                        break
                    question = pair.get("question", "")
                    if core.is_dup_question(question, existing_questions):
                        continue
                    existing_questions.append(question)
                    accepted_pair = {
                        "question": question,
                        "answer": pair.get("answer", ""),
                        "source": pair.get("source", ""),
                        "chunk_text": pair.get("chunk_text", ""),
                    }
                    accepted_pairs.append(accepted_pair)
                    repo.add_pair(
                        generation_id=generation_id,
                        question=accepted_pair["question"],
                        answer=accepted_pair["answer"],
                        source=accepted_pair["source"],
                        chunk_text=accepted_pair["chunk_text"],
                        pair_status="accepted",
                    )

            if progress_callback:
                with shared_lock:
                    pairs_now = len(existing_questions)
                progress_callback(
                    {
                        "message": (
                            f"Completed chunk {completed}/{total_chunks} | "
                            f"Total pairs: {pairs_now}/{final_max_pairs}"
                        ),
                        "chunks_completed": completed,
                        "chunks_total": total_chunks,
                        "pairs_accepted": pairs_now,
                        "pairs_target": final_max_pairs,
                    }
                )

    # Filter out bootstrap placeholders from existing rows.
    return [pair for pair in accepted_pairs if pair.get("answer")]
