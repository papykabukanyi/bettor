web: gunicorn "src.dashboard:app" --bind 0.0.0.0:$PORT --workers 1 --worker-class gthread --threads 8 --timeout 120 --keep-alive 5
