from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import router
from app.services.log_storage_es import ElasticLogStorage
from contextlib import asynccontextmanager
from app.services.log_processor import LogProcessor
from app.utils.kafka_consumer import KafkaLogConsumer

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🔄 Starting application...")
    try:
        # Initialize and store Kafka consumer in app state
        app.state.kafka_consumer = KafkaLogConsumer()
        app.state.kafka_consumer.start()
        print("✅ Kafka consumer started")
        
        yield  # Application is running
        
    finally:
        # Shutdown
        print("🛑 Shutting down application...")
        if hasattr(app.state, 'kafka_consumer'):
            app.state.kafka_consumer.stop()
            print("✅ Kafka consumer stopped")
        
        try:
            if hasattr(router, 'log_processor'):
                await router.log_processor.cleanup()
                print("✅ Log processor cleaned up")
        except Exception as e:
            print(f"❌ Error during shutdown: {e}")

app = FastAPI(title="HDFS Anomaly Detection System", lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    try:
        print("🔄 Initializing Elasticsearch...")
        # Initialize Elasticsearch indices
        storage = ElasticLogStorage()
        await storage.initialize()
        print("✅ Elasticsearch initialized")
    except Exception as e:
        print(f"❌ Error during startup: {e}")
        raise

# Include routers
app.include_router(router)

# Health check endpoint
@app.get("/health")
async def health_check():
    kafka_status = "running" if (
        hasattr(app.state, 'kafka_consumer') and 
        app.state.kafka_consumer._thread and 
        app.state.kafka_consumer._thread.is_alive()
    ) else "not running"
    
    return {
        "status": "healthy",
        "kafka_consumer": kafka_status
    }