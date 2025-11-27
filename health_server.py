#!/usr/bin/env python3
"""
Simple HTTP health check server for Railway deployment.
Responds to health checks while the pipeline runs in the background.
"""

import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

PORT = int(os.environ.get('PORT', 8080))

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/' or self.path == '/health':
            # Health check endpoint
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            response = {
                'status': 'healthy',
                'service': 'crypto-pipeline',
                'timestamp': datetime.utcnow().isoformat(),
                'message': 'Pipeline is running. This service processes Coinbase data and writes to GCS.',
                'processes': [
                    'Coinbase Kafka Producer',
                    'Kafka 1-Min Aggregator', 
                    'GCS Kafka Consumer'
                ]
            }
            
            self.wfile.write(json.dumps(response, indent=2).encode())
        else:
            # 404 for other paths
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def log_message(self, format, *args):
        """Suppress default logging to reduce noise"""
        # Only log errors
        if args[1] != '200':
            super().log_message(format, *args)

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), HealthHandler)
    print(f'Health check server running on port {PORT}')
    print(f'Endpoints: http://0.0.0.0:{PORT}/health')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down health check server...')
        server.shutdown()
