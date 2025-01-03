import React from 'react';

const TimeUnitFilter = ({ timeUnit, setTimeUnit }) => {
  const timeUnits = ['1min', '5min', '10min', '30min', '1h', '24h'];

  return (
    <div className="flex items-center space-x-2">
      <span className="text-sm font-medium text-gray-500">Interval:</span>
      <div className="flex space-x-1">
        {timeUnits.map((unit) => (
          <button
            key={unit}
            onClick={() => setTimeUnit(unit)}
            className={`px-2 py-1 text-xs rounded-md transition-colors ${
              timeUnit === unit
                ? 'bg-blue-500 text-white'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {unit}
          </button>
        ))}
      </div>
    </div>
  );
};

export default TimeUnitFilter; 