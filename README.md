# HDFS Anomaly Detection System

A real-time anomaly detection system for HDFS logs using AI tools including Hugging Face, Chroma, and LangChain. The system includes a modern web interface built with React and Tailwind CSS.

## Prerequisites

- Python 3.9+
- Node.js 16+
- Docker

## Setup Instructions

### Backend Setup

1. Create Docker container for Elasticsearch

```bash
docker compose up -d
```

2. Create and activate a virtual environment:

```bash
cd backend
python -m venv venv
```
On mac/Linux :

```bash
source venv/bin/activate
```
On Windows : 

```
source venv/scripts/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Create .env file with the following variables:

```bash
HUGGING_FACE_API_TOKEN=your_huggingface_api_key
```

4. Create a Kafka topic:

```bash
docker-compose exec kafka kafka-topics --create --topic hadoop-logs --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```


5. Start the backend server:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Setup

1. Navigate to frontend directory:

```bash
cd frontend
```

2. Install dependencies:

```bash
npm install
```

3. Start the development server:

```bash
npm run dev
```

The frontend will be available at http://localhost:3000

## Accessing the Application

- Frontend: http://localhost:5173 (Vite dev server)
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## Development

### Backend Development

The backend is built with FastAPI and uses:

- LangChain for log processing
- Hugging Face transformers for anomaly detection
- Chroma for vector storage
- WebSockets for real-time updates

### Frontend Development

The frontend is built with:

- React + JavaScript
- Vite for build tooling
- Tailwind CSS for styling

## API Endpoints

- `GET /health`: Health check endpoint
- `GET /anomalies/history`: Get historical anomalies
- `POST /simulate-logs`: Simulate logs for testing
- `WS /ws`: WebSocket endpoint for real-time updates

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## TODO

- [x] Add a Database to store logs and anomalies
- [x] Enhace Dashboard with more visualizations
- [ ] Add authentication and authorization
- [ ] Add logs auto collection from HDFS (through a script)
