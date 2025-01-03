import React from 'react';

const StatsCard = ({ title, value, icon }) => {
  return (
    <div className="bg-white rounded-xl h-[160px] transition-all duration-300 hover:shadow-lg border border-gray-100">
      <div className="h-full p-6 flex flex-col justify-between">
        {/* Header with icon */}
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider">
            {title}
          </h3>
          <div className="flex-shrink-0 p-3 bg-gray-50 rounded-lg">
            <div className="w-7 h-7 transform transition-transform duration-300 hover:scale-110">
              {icon}
            </div>
          </div>
        </div>

        {/* Value */}
        <div className="flex items-end">
          <div className="text-3xl font-bold text-gray-900">
            {value}
          </div>
        </div>
      </div>
    </div>
  );
};

export default StatsCard; 