# QnA Pair Generator Web App (Flask)
# Browser-based GUI for uploading text files and generating CSV output

from flask import Flask, render_template, request, jsonify, send_file, Response, stream_with_context
import os
import csv
import json
import tempfile
import threading
import queue
import io
import re
import core
from werkzeug.utils import secure_filename
from agents.graph import run_generation
from generation_db import GenerationRepository

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()

# Global queue for progress updates
progress_queue = queue.Queue()
repo = GenerationRepository()

try:
    retention_days = int(os.getenv("QNA_GEN_RETENTION_DAYS", "7"))
    if retention_days > 0:
        repo.cleanup_old_generations(retention_days)
except Exception:
    pass


def slugify_filename(title: str, original_filename: str) -> str:
    clean = re.sub(r"[^\w\-\s]", "", title or "")
    clean = re.sub(r"\s+", "_", clean).strip("_")
    if clean:
        return f"{clean}.csv"
    base_name = original_filename.replace(".txt", "") if original_filename.endswith(".txt") else original_filename
    return f"{base_name or 'qa_bm_pairs'}.csv"


def write_csv_bytes(pairs, *, title: str, original_filename: str, domain: str, abstract: str, source: str, source_name: str):
    csv_buffer = io.StringIO(newline="")
    writer = csv.writer(csv_buffer)
    writer.writerow(["Soalan", "Jawapan", "Abstract", "Domain", "Sumber", "Potongan_teks"])
    for pair in pairs:
        sumber_value = source if source else source_name
        writer.writerow(
            [
                pair.get("question", ""),
                pair.get("answer", ""),
                abstract,
                domain,
                sumber_value,
                pair.get("chunk_text", ""),
            ]
        )
    csv_filename = slugify_filename(title, original_filename)
    return csv_buffer.getvalue().encode("utf-8"), csv_filename
@app.route('/api/extract', methods=['POST'])
def extract_clean_text():
    """Run prefilter to extract CLEAN_TEXT blocks for preview (TITLE/ABSTRACT/BODY)."""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        if not file.filename.endswith('.txt'):
            return jsonify({'error': 'Only .txt files are supported'}), 400

        full_text = file.read().decode('utf-8')
        src_name = secure_filename(file.filename)

        # Fallback: Check for wrapper tags
        import re
        # Try regular opening/closing tags first
        content_match = re.search(r'<Content>(.*?)</Content>', full_text, flags=re.DOTALL | re.IGNORECASE)
        
        # If not found, try self-closing tag format
        if not content_match:
            content_match = re.search(r'<Content>(.*?)<Content\s*/>', full_text, flags=re.DOTALL | re.IGNORECASE)
        
        if content_match:
            print(f"[DEBUG] Wrapper tags FOUND in {src_name}")
            # Use Content wrapper as BODY_BLOCK
            wrapped_content = content_match.group(1).strip()
            
            # Try to extract Title from <Title> wrapper
            title = ""
            title_wrapper = re.search(r'<Title>(.*?)</Title>', full_text, flags=re.DOTALL | re.IGNORECASE)
            if not title_wrapper:
                title_wrapper = re.search(r'<Title>(.*?)<Title\s*/>', full_text, flags=re.DOTALL | re.IGNORECASE)
            if title_wrapper:
                title = title_wrapper.group(1).strip()
            else:
                # Fallback to regex search for title
                title_match = re.search(r'(?:Tajuk|TITLE)\s*:\s*(.*?)(?:\n|$)', full_text, re.IGNORECASE)
                if title_match:
                    title = title_match.group(1).strip()
            
            # Try to extract Abstract from <Abstract> wrapper
            abstract = ""
            abstract_wrapper = re.search(r'<Abstract>(.*?)</Abstract>', full_text, flags=re.DOTALL | re.IGNORECASE)
            if not abstract_wrapper:
                abstract_wrapper = re.search(r'<Abstract>(.*?)<Abstract\s*/>', full_text, flags=re.DOTALL | re.IGNORECASE)
            if abstract_wrapper:
                abstract = abstract_wrapper.group(1).strip()
            else:
                # Fallback to regex search for abstract
                abstract_match = re.search(r'(?:Abstrak|ABSTRACT)\s*:\s*(.*?)(?:\n|$)', full_text, re.IGNORECASE)
                if abstract_match:
                    abstract = abstract_match.group(1).strip()
            
            # Extract source if available
            source = ""
            source_match = re.search(r'(?:Sumber|SOURCE)\s*:\s*(.*?)(?:\n|$)', full_text, re.IGNORECASE)
            if source_match:
                source = source_match.group(1).strip()
            
            body = wrapped_content
        else:
            print(f"[DEBUG] <Content> wrapper NOT found in {src_name}, using AI extraction")
            # Normal AI extraction
            user_prompt = f"FULL TEXT:\n{full_text}\n\nReturn CLEAN_TEXT blocks as specified."
            raw = core.chat(core.MODEL_GEN, core.PREFILTER_SYSTEM, user_prompt, temperature=0.0)
            # Simple parse of blocks
            title = ""; abstract = ""; source = ""; body = ""
            def extract_block(label: str, text: str) -> str:
                m = re.search(rf"{label}:\s*(.*?)(?:\n\s*\n[A-Z_ ]+:|\Z)", text, flags=re.DOTALL)
                return (m.group(1).strip() if m else "")
            if raw:
                title = extract_block("TITLE", raw)
                abstract = extract_block("ABSTRACT_BLOCK", raw)
                source = extract_block("SOURCE", raw)
                body = extract_block("BODY_BLOCK", raw)

        return jsonify({
            'source_name': src_name,
            'title': title,
            'abstract': abstract,
            'source': source,
            'body': body
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/preview-chunks', methods=['POST'])
def preview_chunks():
    """Preview chunks from extracted clean text (for display only)"""
    try:
        # Get extracted data from the frontend
        abstract = request.form.get('abstract', '')
        body = request.form.get('body', '')
        
        # Combine for chunking
        combined_text = f"{abstract}\n\n{body}".strip()
        
        # Use core's chunking function
        chunks = core.chunk_words(combined_text, core.CHUNK_WORDS, core.CHUNK_OVERLAP)
        
        # Format for display
        chunks_display = []
        for idx, (chunk_text, start_word, end_word) in enumerate(chunks, 1):
            # Preview first 200 characters of each chunk
            preview = chunk_text[:200] + '...' if len(chunk_text) > 200 else chunk_text
            chunks_display.append({
                'index': idx,
                'preview': preview,
                'full_text': chunk_text,
                'word_range': f"{start_word}-{end_word}",
                'word_count': len(chunk_text.split())
            })
        
        return jsonify({
            'total_chunks': len(chunks),
            'chunks': chunks_display
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    """Render the main page"""
    return render_template('index.html')

@app.route('/api/generate', methods=['POST'])
def generate_qa():
    """Process uploaded file and generate Q&A pairs with live progress"""
    try:
        # Accept file or CLEAN_TEXT blocks
        file = request.files.get('file')
        title_field = request.form.get('title')
        abstract_field = request.form.get('abstract')
        source_field = request.form.get('source')
        body_field = request.form.get('body')
        if not file and not (title_field or abstract_field or body_field):
            return jsonify({'error': 'No input provided'}), 400
        
        # Get all settings from request (capture before background thread)
        # max_pairs is optional - if empty or 0, use adaptive only; otherwise use as cap
        max_pairs_str = request.form.get('max_pairs', '').strip()
        max_pairs = int(max_pairs_str) if max_pairs_str and max_pairs_str != '0' else None
        skip_review = request.form.get('skip_review', 'true').lower() == 'true'
        requested_generation_id = (request.form.get('generation_id') or '').strip()
        domain = (request.form.get('domain') or 'Sejarah').strip()
        
        # Read content
        if title_field is not None or abstract_field is not None or body_field is not None:
            body = body_field or ''
            abstract = abstract_field or ''
            source = source_field or ''
            file_content = (abstract + "\n\n" + body).strip()
            source_name = secure_filename(request.form.get('source_name') or 'uploaded.txt')
            original_filename = source_name
            doc_title = (title_field or '').strip() or source_name
        else:
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            if not file.filename.endswith('.txt'):
                return jsonify({'error': 'Only .txt files are supported'}), 400
            file_content = file.read().decode('utf-8')
            source_name = secure_filename(file.filename)
            original_filename = file.filename
            doc_title = source_name
            abstract = ''  # No abstract when reading raw file
            source = ''  # No source when reading raw file
        
        generation_metadata = {
            'original_filename': original_filename,
            'source_name': source_name,
            'title': doc_title,
            'domain': domain,
            'abstract': abstract,
            'source': source,
        }
        generation_id = requested_generation_id
        existing_generation = repo.get_generation(generation_id) if generation_id else None
        if generation_id and not existing_generation:
            return jsonify({'error': 'Invalid generation_id'}), 404
        if not generation_id:
            generation_id = repo.create_generation(generation_metadata)
        else:
            repo.upsert_generation_metadata(generation_id, generation_metadata)
            repo.update_generation_status(generation_id, 'running')

        # Clear progress queue
        while not progress_queue.empty():
            try:
                progress_queue.get_nowait()
            except:
                pass
        
        def generate_with_progress():
            """Generate Q&A pairs and send progress updates"""
            pairs = []
            
            def progress_callback(payload):
                """Callback to send progress updates (string or structured dict)."""
                if isinstance(payload, dict):
                    msg = payload.get('message', '')
                    repo.add_event(generation_id, 'progress', json.dumps(payload, ensure_ascii=False))
                    item = {'type': 'progress', 'message': msg}
                    for key in ('chunks_completed', 'chunks_total', 'pairs_accepted', 'pairs_target'):
                        if key in payload:
                            item[key] = payload[key]
                    progress_queue.put(item)
                else:
                    repo.add_event(generation_id, 'progress', str(payload))
                    progress_queue.put({'type': 'progress', 'message': str(payload)})
            
            try:
                # Process the file with progress callback
                new_pairs = run_generation(
                    repo,
                    generation_id,
                    file_content,
                    source_name,
                    max_pairs=max_pairs,
                    progress_callback=progress_callback,
                    skip_review=skip_review,
                    max_workers=int(os.getenv("QNA_MAX_WORKERS", "10")),
                    doc_title=doc_title
                )
                all_pairs = repo.list_pairs(generation_id)
                pairs = [
                    {
                        'id': p['id'],
                        'question': p['question'],
                        'answer': p['answer'],
                        'source': p['source'],
                        'chunk_text': p.get('chunk_text', ''),
                        'pair_status': p.get('pair_status', 'accepted'),
                    }
                    for p in all_pairs
                ]
                repo.update_generation_status(generation_id, 'complete')
                repo.add_event(generation_id, 'complete', f'Generated {len(new_pairs)} new pairs')
                
                # Send completion with file info
                progress_queue.put({
                    'type': 'complete',
                    'generation_id': generation_id,
                    'pairs': pairs,
                    'count': len(pairs),
                    'new_pairs_count': len(new_pairs),
                    'original_filename': original_filename,
                    'file_size': len(file_content),
                    'word_count': len(file_content.split()),
                    'abstract': abstract,
                    'source': source,
                    'source_name': source_name
                })
            except ValueError as e:
                # Handle API errors specifically
                error_msg = str(e)
                repo.update_generation_status(generation_id, 'error')
                progress_queue.put({
                    'type': 'error',
                    'error': error_msg,
                    'is_rate_limit': 'rate limit' in error_msg.lower(),
                    'is_auth_error': 'invalid api key' in error_msg.lower()
                })
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                print(f"Error in generation: {error_details}")
                repo.update_generation_status(generation_id, 'error')
                progress_queue.put({
                    'type': 'error',
                    'error': f"{str(e)}\n\nCheck server logs for details."
                })
        
        # Start generation in background thread
        thread = threading.Thread(target=generate_with_progress)
        thread.daemon = True
        thread.start()
        
        # Return streaming response
        def event_stream():
            while True:
                try:
                    # Get progress update with timeout
                    data = progress_queue.get(timeout=1)
                    
                    if data['type'] == 'complete':
                        yield f"data: {json.dumps(data)}\n\n"
                        break
                    elif data['type'] == 'error':
                        yield f"data: {json.dumps(data)}\n\n"
                        break
                    else:
                        yield f"data: {json.dumps(data)}\n\n"
                except queue.Empty:
                    # Send heartbeat to keep connection alive
                    yield ": heartbeat\n\n"
                    continue
        
        return Response(stream_with_context(event_stream()), mimetype='text/event-stream')
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-csv', methods=['POST'])
def download_csv():
    """Generate and download CSV file"""
    try:
        data = request.json or {}
        generation_id = (data.get('generation_id') or '').strip()

        if generation_id:
            generation = repo.get_generation(generation_id)
            if not generation:
                return jsonify({'error': 'Generation not found'}), 404
            metadata = generation.get('metadata', {})
            db_pairs = repo.list_pairs(generation_id)
            pairs = [
                {
                    'question': p['question'],
                    'answer': p['answer'],
                    'chunk_text': p.get('chunk_text', ''),
                }
                for p in db_pairs
            ]
            original_filename = (metadata.get('original_filename') or 'qa_bm_pairs').strip()
            title = (metadata.get('title') or '').strip()
            domain = (metadata.get('domain') or 'Sejarah').strip()
            abstract = (metadata.get('abstract') or '').strip()
            source = (metadata.get('source') or '').strip()
            source_name = (metadata.get('source_name') or original_filename).strip()
        else:
            pairs = data.get('pairs', [])
            original_filename = data.get('original_filename', 'qa_bm_pairs')
            title = (data.get('title') or '').strip()
            domain = data.get('domain', 'Sejarah').strip()
            abstract = (data.get('abstract') or '').strip()
            source = (data.get('source') or '').strip()
            source_name = (data.get('source_name') or original_filename).strip()

        if not pairs:
            return jsonify({'error': 'No data to export'}), 400

        csv_bytes, csv_filename = write_csv_bytes(
            pairs,
            title=title,
            original_filename=original_filename,
            domain=domain,
            abstract=abstract,
            source=source,
            source_name=source_name,
        )

        temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
        temp_file.write(csv_bytes)
        temp_file.close()
        return send_file(
            temp_file.name,
            mimetype='text/csv',
            as_attachment=True,
            download_name=csv_filename
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/generations/<generation_id>/pairs', methods=['GET'])
def list_generation_pairs(generation_id):
    generation = repo.get_generation(generation_id)
    if not generation:
        return jsonify({'error': 'Generation not found'}), 404
    pairs = repo.list_pairs(generation_id)
    return jsonify({
        'generation_id': generation_id,
        'status': generation.get('status'),
        'pairs': pairs,
    })


@app.route('/api/generations/<generation_id>/pairs/<int:pair_id>', methods=['PATCH'])
def update_generation_pair(generation_id, pair_id):
    data = request.json or {}
    question = (data.get('question') or '').strip()
    answer = (data.get('answer') or '').strip()
    if not question or not answer:
        return jsonify({'error': 'Both question and answer are required'}), 400
    if not repo.get_generation(generation_id):
        return jsonify({'error': 'Generation not found'}), 404
    updated = repo.update_pair(generation_id, pair_id, question, answer)
    if not updated:
        return jsonify({'error': 'Pair not found'}), 404
    return jsonify({'ok': True})


@app.route('/api/generations/<generation_id>/pairs/<int:pair_id>', methods=['DELETE'])
def delete_generation_pair(generation_id, pair_id):
    if not repo.get_generation(generation_id):
        return jsonify({'error': 'Generation not found'}), 404
    deleted = repo.delete_pair(generation_id, pair_id)
    if not deleted:
        return jsonify({'error': 'Pair not found'}), 404
    return jsonify({'ok': True})

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check if API is configured"""
    has_config = bool(core.API_KEY and core.BASE_URL)
    return jsonify({
        'configured': has_config,
        'model_gen': core.MODEL_GEN,
        'model_review': core.MODEL_REVIEW
    })

@app.route('/api/verify-connection', methods=['GET'])
def verify_connection():
    """Verify AI API connection by making a test call"""
    try:
        if not core.API_KEY or not core.BASE_URL:
            return jsonify({
                'connected': False,
                'error': 'API credentials not configured'
            })
        
        # Make a simple test call
        test_response = core.chat(
            core.MODEL_GEN,
            "You are a helpful assistant.",
            "Say 'OK' if you can read this.",
            temperature=0.1
        )
        
        if test_response and len(test_response) > 0:
            return jsonify({
                'connected': True,
                'model': core.MODEL_GEN,
                'message': 'Successfully connected to AI API'
            })
        else:
            return jsonify({
                'connected': False,
                'error': 'No response from API'
            })
    except ValueError as e:
        # Handle specific API errors
        error_msg = str(e)
        return jsonify({
            'connected': False,
            'error': error_msg,
            'is_rate_limit': 'rate limit' in error_msg.lower(),
            'is_auth_error': 'invalid api key' in error_msg.lower()
        }), 200
    except Exception as e:
        return jsonify({
            'connected': False,
            'error': str(e)
        }), 200

if __name__ == '__main__':
    # Check if API is configured
    if not core.API_KEY or not core.BASE_URL:
        print("\n" + "="*60)
        print("WARNING: API credentials not configured!")
        print("Please set OPENAI_API_KEY and OPENAI_BASE_URL in your .env file")
        print("="*60 + "\n")
    
    # Try port 8080 first, fallback to 5001 if needed
    port = 8080
    print("Starting QnA Pair Generator Web App...")
    print(f"Open your browser and navigate to: http://localhost:{port}")
    app.run(debug=True, host='0.0.0.0', port=port)

