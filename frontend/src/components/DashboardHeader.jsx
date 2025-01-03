import React from 'react';

const DashboardHeader = () => {
  return (
    <div className="mb-8 text-center">
      <h1 className="text-4xl font-bold bg-gradient-to-r from-gray-800 via-gray-700 to-gray-600 bg-clip-text text-transparent">
        Anomaly Detection System
      </h1>
      <div className="mt-2 flex justify-center">
        <div className="h-1 w-32 bg-gradient-to-r from-gray-800 via-gray-700 to-gray-600 rounded-full"></div>
      </div>
    </div>
  );
};

export default DashboardHeader; 