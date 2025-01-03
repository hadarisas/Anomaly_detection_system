import React from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import TimeUnitFilter from './TimeUnitFilter';

const LiveAnomalyChart = ({ chartData, timeUnit, setTimeUnit, isConnected }) => {
  // Get visible points based on time unit
  const getVisiblePoints = () => {
    if (!chartData.length) return [];
    
    const now = new Date();
    const timeWindow = {
      '1min': 60 * 1000,
      '5min': 5 * 60 * 1000,
      '10min': 10 * 60 * 1000,
      '30min': 30 * 60 * 1000,
      '1h': 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000
    }[timeUnit];

    // Filter data points within the time window
    const cutoff = now.getTime() - timeWindow;
    const recentPoints = chartData.filter(point => point.timestamp.getTime() > cutoff);

    // Group data points by time buckets
    const buckets = recentPoints.reduce((acc, point) => {
      const bucketTime = new Date(point.timestamp);
      switch(timeUnit) {
        case '5min':
          bucketTime.setMinutes(Math.floor(bucketTime.getMinutes() / 5) * 5, 0, 0);
          break;
        case '10min':
          bucketTime.setMinutes(Math.floor(bucketTime.getMinutes() / 10) * 10, 0, 0);
          break;
        case '30min':
          bucketTime.setMinutes(Math.floor(bucketTime.getMinutes() / 30) * 30, 0, 0);
          break;
        case '1h':
          bucketTime.setMinutes(0, 0, 0);
          break;
        case '24h':
          bucketTime.setHours(bucketTime.getHours(), 0, 0, 0);
          break;
        default: // 1min
          bucketTime.setSeconds(0, 0);
      }

      const key = bucketTime.getTime();
      
      if (!acc[key]) {
        acc[key] = {
          timestamp: bucketTime,
          critical: 0,
          warning: 0
        };
      }
      
      acc[key].critical += point.critical;
      acc[key].warning += point.warning;
      
      return acc;
    }, {});

    return Object.values(buckets)
      .sort((a, b) => a.timestamp - b.timestamp)
      .slice(-12);
  };

  const formatXAxis = (timestamp) => {
    if (!(timestamp instanceof Date)) return '';
    
    switch(timeUnit) {
      case '1min':
        return timestamp.toLocaleTimeString([], { 
          hour: '2-digit', 
          minute: '2-digit' 
        });
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

  const visibleData = getVisiblePoints();
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
              interval="preserveEnd"
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