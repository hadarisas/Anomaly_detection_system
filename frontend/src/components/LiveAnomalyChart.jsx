import React from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import TimeUnitFilter from './TimeUnitFilter';

const LiveAnomalyChart = ({ chartData, timeUnit, setTimeUnit, isConnected }) => {
  // Generate timeline points based on time unit
  const generateTimelinePoints = () => {
    if (!chartData.length) return [];
    
    const now = new Date();
    const intervals = {
    
      '5min': 12,
      '10min': 6,
      '30min': 2,
      '1h': 1,
      '24h': 24
    };
    
    const points = [];
    const numPoints = intervals[timeUnit];
    
    // Calculate interval in milliseconds
    const intervalMs = {
    
      '5min': 5 * 60 * 1000,
      '10min': 10 * 60 * 1000,
      '30min': 30 * 60 * 1000,
      '1h': 60 * 60 * 1000,
      '24h': 60 * 60 * 1000
    }[timeUnit];
    
    // Generate points for the full timeline
    for (let i = numPoints - 1; i >= 0; i--) {
      const pointTime = new Date(now.getTime() - (i * intervalMs));
      
      // Find matching data point or use empty values
      const dataPoint = chartData.find(d => {
        const diff = Math.abs(d.timestamp.getTime() - pointTime.getTime());
        return diff < intervalMs / 2;
      });
      
      points.push({
        timestamp: pointTime,
        critical: dataPoint?.critical || 0,
        warning: dataPoint?.warning || 0
      });
    }
    
    return points;
  };

  const formatXAxis = (timestamp) => {
    if (!(timestamp instanceof Date)) return '';
    
    switch(timeUnit) {
      case '5min':
      case '10min':
      case '30min':
      case '1h':
        return timestamp.toLocaleTimeString([], { 
          hour: '2-digit', 
          minute: '2-digit'
        });
      case '24h':
        return timestamp.toLocaleTimeString([], { 
          hour: '2-digit'
        });
      default:
        return timestamp.toLocaleTimeString([], { 
          hour: '2-digit', 
          minute: '2-digit' 
        });
    }
  };

  const getYAxisTicks = (data) => {
    if (!data.length) return [0];
    
    const maxValue = Math.max(
      ...data.map(point => Math.max(point.critical, point.warning))
    );
    
    if (maxValue === 0) return [0];
    
    const tickCount = 5;
    const interval = Math.ceil(maxValue / (tickCount - 1));
    
    return Array.from(
      { length: tickCount }, 
      (_, i) => i * interval
    );
  };

  const visibleData = generateTimelinePoints();
  const yAxisTicks = getYAxisTicks(visibleData);

  return (
    <div className="bg-white shadow rounded-lg p-4 mb-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium text-gray-900">
          Live Anomaly Chart {isConnected ? '(Connected)' : '(Disconnected)'}
        </h2>
        <TimeUnitFilter timeUnit={timeUnit} setTimeUnit={setTimeUnit} />
      </div>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={visibleData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="timestamp"
              tickFormatter={formatXAxis}
              interval={0}
              angle={-45}
              textAnchor="end"
              height={60}
            />
            <YAxis 
              domain={[0, Math.max(...yAxisTicks)]}
              ticks={yAxisTicks}
              allowDataOverflow={false}
              label={{ 
                value: 'Number of Anomalies', 
                angle: -90, 
                position: 'insideLeft',
                style: { textAnchor: 'middle' }
              }}
            />
            <Tooltip 
              formatter={(value, name) => [`${Math.floor(value)} anomalies`, name]}
              labelFormatter={(timestamp) => `Time: ${formatXAxis(timestamp)}`}
            />
            <Legend verticalAlign="top" height={36} />
            <Line
              type="monotone"
              dataKey="critical"
              stroke="#EF4444"
              name="Critical Anomalies"
              isAnimationActive={false}
              dot={false}
              strokeWidth={2}
            />
            <Line
              type="monotone"
              dataKey="warning"
              stroke="#F59E0B"
              name="Warning Anomalies"
              isAnimationActive={false}
              dot={false}
              strokeWidth={2}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default LiveAnomalyChart; 