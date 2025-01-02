import { useState, useEffect, useCallback } from "react";

function Dashboard() {
  const [anomalies, setAnomalies] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isSimulating, setIsSimulating] = useState(false);
  const [ws, setWs] = useState(null);

  // Initialize WebSocket connection
  useEffect(() => {
    const websocket = new WebSocket("ws://localhost:8000/ws");

    websocket.onopen = () => {
      console.log("Connected to WebSocket");
      setIsConnected(true);
    };

    websocket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (Array.isArray(data)) {
          setAnomalies((prev) => [...prev, ...data]);
        } else if (data.status === "simulation_stopped") {
          setIsSimulating(false);
        }
      } catch (error) {
        console.error("Error parsing WebSocket message:", error);
      }
    };

    websocket.onclose = () => {
      console.log("Disconnected from WebSocket");
      setIsConnected(false);
      setIsSimulating(false);
    };

    setWs(websocket);

    return () => {
      websocket.close();
    };
  }, []);

  // Handle simulation control
  const toggleSimulation = useCallback(async () => {
    try {
      if (!isSimulating) {
        console.log("Starting simulation..."); // Debug log
        setIsSimulating(true);
        setAnomalies([]); // Clear previous anomalies
        ws?.send(JSON.stringify({ action: "start_simulation" }));
      } else {
        console.log("Stopping simulation..."); // Debug log
        ws?.send(JSON.stringify({ action: "stop_simulation" }));
        setIsSimulating(false);
      }
    } catch (error) {
      console.error("Error toggling simulation:", error);
      setIsSimulating(false);
    }
  }, [isSimulating, ws]);

  // Clear anomalies
  const clearAnomalies = useCallback(() => {
    setAnomalies([]);
  }, []);

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="max-w-7xl mx-auto py-6 sm:px-6 lg:px-8">
        <div className="mb-6 bg-white shadow rounded-lg p-4">
          <h2 className="text-lg font-medium text-gray-900 mb-4">
            Control Panel
          </h2>
          <div className="flex items-center space-x-4">
            <button
              onClick={toggleSimulation}
              disabled={!isConnected}
              className={`px-4 py-2 rounded-md ${
                !isConnected
                  ? "bg-gray-400 cursor-not-allowed"
                  : isSimulating
                  ? "bg-red-500 hover:bg-red-600"
                  : "bg-green-500 hover:bg-green-600"
              } text-white transition-colors`}
            >
              {isSimulating ? "Stop Simulation" : "Start Simulation"}
            </button>
            <button
              onClick={clearAnomalies}
              className="px-4 py-2 rounded-md bg-blue-500 hover:bg-blue-600 text-white transition-colors"
            >
              Clear Anomalies
            </button>
            <div className="flex items-center">
              <div
                className={`h-3 w-3 rounded-full mr-2 ${
                  isConnected ? "bg-green-500" : "bg-red-500"
                }`}
              />
              <span className="text-sm text-gray-500">
                {isConnected ? "Connected" : "Disconnected"}
              </span>
            </div>
          </div>
        </div>

        <div className="px-4 py-6 sm:px-0">
          <div className="border-4 border-dashed border-gray-200 rounded-lg p-4">
            {anomalies.length === 0 ? (
              <div className="text-center text-gray-500 py-8">
                No anomalies detected yet
              </div>
            ) : (
              <div className="space-y-4">
                {anomalies.map((anomaly, index) => (
                  <div
                    key={index}
                    className="bg-white shadow rounded-lg p-4 hover:shadow-lg transition-shadow"
                  >
                    <div className="flex justify-between items-start">
                      <div className="flex-1">
                        <h3 className="text-lg font-medium text-gray-900">
                          Anomaly Detected
                        </h3>
                        <p className="mt-1 text-sm text-gray-500">
                          {anomaly.text}
                        </p>
                      </div>
                      <div className="ml-4">
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                          Score: {anomaly.score.toFixed(2)}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
