import React from 'react';
import { FaExclamationTriangle, FaServer, FaChartLine, FaExclamationCircle } from 'react-icons/fa';
import StatsCard from './StatsCard';

const StatsGrid = ({ stats }) => {
  return (
    <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4 mb-8">
      <StatsCard
        title="Total Anomalies"
        value={stats.totalAnomalies}
        icon={<FaExclamationCircle className="text-blue-500 h-6 w-6" />}
      />
      <StatsCard
        title="Critical Alerts"
        value={stats.criticalCount}
        icon={<FaServer className="text-red-500 h-6 w-6" />}
      />
      <StatsCard
        title="Warnings"
        value={stats.warningCount}
        icon={<FaExclamationTriangle className="text-yellow-500 h-6 w-6" />}
      />
      <StatsCard
        title="Average Score"
        value={stats.averageScore.toFixed(2)}
        icon={<FaChartLine className="text-orange-500 h-6 w-6" />}
      />
    </div>
  );
};

export default StatsGrid; 