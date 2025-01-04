import React, { useState, useEffect, useRef, useCallback } from 'react';
import RecentAnomalies from './RecentAnomalies';
import LiveAnomalyChart from './LiveAnomalyChart';
import ControlPanel from './ControlPanel';
import StatsGrid from './StatsGrid';
import DashboardHeader from './DashboardHeader';
import HistoricalChart from './HistoricalChart';

const Dashboard = () => {
  const [isSimulating, setIsSimulating] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [chartData, setChartData] = useState([]);
  const [recentAnomalies, setRecentAnomalies] = useState([]);
  const [timeUnit, setTimeUnit] = useState('10min');
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
          text: anomaly.text
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

        // Keep last 60 minutes of data
        const cutoff = new Date(now.getTime() - 60 * 60 * 1000);
        return [...prevData.filter(point => point.timestamp > cutoff), newPoint];
      });
    }
  }, [updateStats]);

   // Fetch recent anomalies on component mount
   useEffect(() => {
    const fetchRecentAnomalies = async () => {
      try {
        const response = await fetch('http://localhost:8000/anomalies/logs/10');
        if (!response.ok) throw new Error('Failed to fetch recent anomalies');
        const data = await response.json();
        console.log('Recent anomalies fetched:', data);
        setRecentAnomalies(data);
      } catch (error) {
        console.error('Error fetching recent anomalies:', error);
      }
    };

    fetchRecentAnomalies();
  }, []);

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

  // Fetch initial chart data
  useEffect(() => {
    const fetchInitialChartData = async () => {
      try {
        const response = await fetch(`http://localhost:8000/anomalies/recent?time_unit=${timeUnit}`);
        if (!response.ok) throw new Error('Failed to fetch chart data');
        const data = await response.json();
        console.log('Initial chart data:', data);
        
        // Convert the totals into a single data point
        const initialDataPoint = {
          timestamp: new Date(data.query_details.end_time),
          critical: data.totals.critical,
          warning: data.totals.warning
        };
        
        setChartData([initialDataPoint]);
        
        // Update stats
        setStats(prevStats => ({
          ...prevStats,
          totalAnomalies: data.totals.critical + data.totals.warning,
          criticalCount: data.totals.critical,
          warningCount: data.totals.warning
        }));
      } catch (error) {
        console.error('Error fetching initial chart data:', error);
      }
    };
    
    fetchInitialChartData();
  }, [timeUnit]);

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
        <HistoricalChart />
        <RecentAnomalies recentAnomalies={recentAnomalies} />
      </div>
    </div>
  );
};

export default Dashboard; 