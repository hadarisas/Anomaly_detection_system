from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import router
from app.services.log_storage_es import ElasticLogStorage
from contextlib import asynccontextmanager
from app.services.log_processor import LogProcessor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown
    try:
        if hasattr(router, 'log_processor'):
            await router.log_processor.cleanup()
    except Exception as e:
        print(f"Error during shutdown: {e}")

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
    # Initialize Elasticsearch indices
    storage = ElasticLogStorage()
    await storage.initialize()

# Include routers
app.include_router(router)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"} 