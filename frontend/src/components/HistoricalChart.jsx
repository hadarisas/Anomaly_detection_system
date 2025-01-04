import React, { useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

const HistoricalChart = () => {
  const [startDate, setStartDate] = useState(
    new Date(new Date().setHours(new Date().getHours() - 24))
  );
  const [endDate, setEndDate] = useState(new Date());
  const [historicalData, setHistoricalData] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  // Color mapping for different anomaly types
  const colorMap = {
    "PERFORMANCE": "#3B82F6", // Blue
    "SECURITY": "#EF4444",    // Red
    "AVAILABILITY": "#10B981", // Green
    "DATA": "#F59E0B",        // Amber
    "NETWORK": "#6366F1",     // Indigo
    "RESOURCE": "#8B5CF6",    // Purple
    "UNKNOWN": "#6B7280"      // Gray
  };

  const fetchHistoricalData = async () => {
    try {
      setIsLoading(true);
      let url = "http://localhost:8000/anomalies/history";

      // Only add date parameters if both dates are selected
      if (startDate && endDate) {
        const formattedStart = startDate.toISOString();
        const formattedEnd = endDate.toISOString();
        url += `?start=${formattedStart}&end=${formattedEnd}`;
      }

      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      console.log("Historical data received:", data);

      // Transform the totals data into individual bars
      const transformedData = Object.entries(data.totals).map(([key, value]) => ({
        name: key,
        value: value
      }));

      console.log("Transformed data:", transformedData);
      setHistoricalData(transformedData);
    } catch (error) {
      console.error("Error fetching historical data:", error);
      setHistoricalData([]);
    } finally {
      setIsLoading(false);
    }
  };

  const CustomBar = (props) => {
    const { fill, x, y, width, height } = props;
    return (
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={colorMap[props.name] || fill}
      />
    );
  };

  return (
    <div className="bg-white shadow rounded-lg p-4 mb-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium text-gray-900">
          Historical Anomalies {historicalData.length > 0 && `(Total: ${historicalData.reduce((sum, item) => sum + item.value, 0)})`}
        </h2>
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2">
            <label className="text-sm text-gray-600">From:</label>
            <DatePicker
              selected={startDate}
              onChange={(date) => setStartDate(date)}
              showTimeSelect
              timeFormat="HH:mm"
              timeIntervals={15}
              dateFormat="yyyy-MM-dd HH:mm"
              className="px-3 py-2 border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              maxDate={endDate}
            />
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm text-gray-600">To:</label>
            <DatePicker
              selected={endDate}
              onChange={(date) => setEndDate(date)}
              showTimeSelect
              timeFormat="HH:mm"
              timeIntervals={15}
              dateFormat="yyyy-MM-dd HH:mm"
              className="px-3 py-2 border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              minDate={startDate}
            />
          </div>
          <button
            onClick={fetchHistoricalData}
            disabled={isLoading}
            className={`px-4 py-2 rounded-md text-white transition-colors ${
              isLoading
                ? "bg-gray-300 cursor-not-allowed"
                : "bg-blue-500 hover:bg-blue-600"
            }`}
          >
            {isLoading ? "Loading..." : "Get Data"}
          </button>
        </div>
      </div>
      <div className="h-64">
        {historicalData.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={historicalData}
              margin={{ top: 20, right: 30, left: 20, bottom: 40 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" angle={-45} textAnchor="end" height={60} />
              <YAxis />
              <Tooltip
                contentStyle={{
                  backgroundColor: "rgba(255, 255, 255, 0.95)",
                  border: "none",
                  borderRadius: "4px",
                  boxShadow: "0 2px 5px rgba(0,0,0,0.15)",
                }}
                formatter={(value, name) => [`${value} anomalies`, name]}
              />
             
              <Bar dataKey="value" name="Count" shape={<CustomBar />} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-full flex items-center justify-center text-gray-500">
            {isLoading ? "Loading..." : "Click 'Get Data' to view historical anomalies"}
          </div>
        )}
      </div>
    </div>
  );
};

export default HistoricalChart;
