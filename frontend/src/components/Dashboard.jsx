import React, { useState, useEffect, useRef, useCallback } from 'react';
import RecentAnomalies from './RecentAnomalies';
import LiveAnomalyChart from './LiveAnomalyChart';
import ControlPanel from './ControlPanel';
import StatsGrid from './StatsGrid';
import DashboardHeader from './DashboardHeader';

const Dashboard = () => {
  const [isSimulating, setIsSimulating] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [chartData, setChartData] = useState([]);
  const [recentAnomalies, setRecentAnomalies] = useState([]);
  const [timeUnit, setTimeUnit] = useState('5min');
  const [stats, setStats] = useState({
    totalAnomalies: 0,
    criticalCount: 0,
    warningCount: 0,
    averageScore: 0,
  });

  const ws = useRef(null);
  const reconnectAttempts = useRef(0);
  const reconnectTimeoutRef = useRef(null);
  const maxReconnectAttempts = 5;
  const reconnectDelay = 2000;

  const connectWebSocket = useCallback(() => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      console.log("WebSocket already connected");
      return;
    }

    if (reconnectAttempts.current >= maxReconnectAttempts) {
      console.log("Max reconnection attempts reached");
      return;
    }

    try {
      console.log(`Connecting to WebSocket (attempt ${reconnectAttempts.current + 1}/${maxReconnectAttempts})`);
      const socket = new WebSocket('ws://localhost:8000/ws');

      socket.onopen = () => {
        console.log('WebSocket Connected');
        setIsConnected(true);
        reconnectAttempts.current = 0;
        ws.current = socket;
      };

      socket.onclose = (event) => {
        setIsConnected(false);
        if (event.wasClean) {
          console.log('WebSocket closed cleanly');
        } else {
          console.log('WebSocket connection lost');
          reconnectAttempts.current++;
          if (reconnectAttempts.current < maxReconnectAttempts) {
            reconnectTimeoutRef.current = setTimeout(connectWebSocket, reconnectDelay);
          }
        }
      };

      socket.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      socket.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          handleWebSocketMessage(data);
        } catch (error) {
          console.error('Error processing WebSocket message:', error);
        }
      };

    } catch (error) {
      console.error('Error creating WebSocket:', error);
      setIsConnected(false);
    }
  }, []);

  const updateStats = useCallback((anomalies) => {
    if (!Array.isArray(anomalies)) return;

    setStats(prevStats => {
      const newCritical = anomalies.filter(a => a.score >= 0.8).length;
      const newWarnings = anomalies.filter(a => a.score < 0.8).length;
      
      const totalScores = anomalies.reduce((sum, a) => sum + (a.score || 0), 0);
      const averageScore = anomalies.length > 0 ? totalScores / anomalies.length : 0;

      return {
        totalAnomalies: prevStats.totalAnomalies + anomalies.length,
        criticalCount: prevStats.criticalCount + newCritical,
        warningCount: prevStats.warningCount + newWarnings,
        averageScore: averageScore,
      };
    });
  }, []);

  const getTimeBucket = useCallback((date, unit) => {
    const minutes = date.getMinutes();
    const hours = date.getHours();
    
    switch(unit) {
      case '1min':
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      case '5min':
        return date.toLocaleTimeString([], { 
          hour: '2-digit', 
          minute: '2-digit' 
        }).replace(/:(\d+)/, (_, m) => `:${Math.floor(minutes / 5) * 5}`.padStart(3, ':0'));
      case '10min':
        return date.toLocaleTimeString([], { 
          hour: '2-digit', 
          minute: '2-digit' 
        }).replace(/:(\d+)/, (_, m) => `:${Math.floor(minutes / 10) * 10}`.padStart(3, ':0'));
      case '30min':
        return date.toLocaleTimeString([], { 
          hour: '2-digit', 
          minute: '2-digit' 
        }).replace(/:(\d+)/, (_, m) => `:${Math.floor(minutes / 30) * 30}`.padStart(3, ':0'));
      case '1h':
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
          .replace(/:\d+/, ':00');
      case '24h':
        return date.toLocaleTimeString([], { hour: '2-digit' }) + ':00';
      default:
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }
  }, []);

  const handleWebSocketMessage = useCallback((data) => {
    if (data.type === 'simulation_status') {
      console.log('Received simulation status:', data);
      return;
    }

    if (Array.isArray(data)) {
      console.log('Received anomaly data:', data);

      // Update recent anomalies
      setRecentAnomalies(prevAnomalies => {
        const now = new Date();
        const newAnomalies = data.map(anomaly => ({
          ...anomaly,
          timestamp: now,
          text: anomaly.text || anomaly.message || anomaly.log || anomaly.details // Capture all possible text fields
        }));
        
        return [...newAnomalies, ...prevAnomalies]
          .sort((a, b) => b.timestamp - a.timestamp)
          .slice(0, 10);
      });

      updateStats(data);

      setChartData(prevData => {
        const now = new Date();
        const criticalAnomalies = data.filter(a => a.score >= 0.8).length;
        const warningAnomalies = data.filter(a => a.score < 0.8).length;

        const newPoint = {
          timestamp: now,
          critical: criticalAnomalies,
          warning: warningAnomalies
        };

        return [...prevData, newPoint].slice(-60);
      });
    }
  }, [updateStats]);

  useEffect(() => {
    connectWebSocket();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (ws.current) {
        ws.current.close();
        ws.current = null;
      }
      setIsConnected(false);
    };
  }, [connectWebSocket]);

  const toggleSimulation = useCallback(() => {
    if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
      console.log("WebSocket not connected");
      return;
    }

    const action = isSimulating ? 'stop_simulation' : 'start_simulation';
    ws.current.send(JSON.stringify({ action }));
    setIsSimulating(!isSimulating);
  }, [isSimulating]);

  const clearStats = useCallback(() => {
    setStats({
      totalAnomalies: 0,
      criticalCount: 0,
      warningCount: 0,
      averageScore: 0,
    });
    setChartData([]);
    setRecentAnomalies([]);
  }, []);

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">
        <DashboardHeader />
        
        <ControlPanel 
          isConnected={isConnected}
          isSimulating={isSimulating}
          toggleSimulation={toggleSimulation}
          clearStats={clearStats}
        />

        <StatsGrid stats={stats} />

        <LiveAnomalyChart 
          chartData={chartData}
          timeUnit={timeUnit}
          setTimeUnit={setTimeUnit}
          isConnected={isConnected}
        />
        <RecentAnomalies recentAnomalies={recentAnomalies} />
      </div>
    </div>
  );
};

export default Dashboard; 