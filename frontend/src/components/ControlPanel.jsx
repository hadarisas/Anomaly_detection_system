import React from 'react';

const ControlPanel = ({ isConnected, isSimulating, toggleSimulation, clearStats }) => {
  return (
    <div className="mb-6 bg-white shadow-lg rounded-xl overflow-hidden">
      <div className="p-6">
        <div className="flex flex-col sm:flex-row items-center justify-between space-y-4 sm:space-y-0 sm:space-x-4">
          <div className="flex items-center space-x-2">
            <div className={`h-3 w-3 rounded-full ${
              isConnected ? 'bg-green-500' : 'bg-red-500'
            } animate-pulse`}></div>
            <span className={`text-sm font-medium ${
              isConnected ? 'text-green-600' : 'text-red-600'
            }`}>
              {isConnected ? 'Connected' : 'Disconnected'}
            </span>
          </div>

          <div className="flex items-center space-x-4">
            <button
              onClick={toggleSimulation}
              disabled={!isConnected}
              className={`
                px-6 py-2.5 rounded-lg font-medium text-white shadow-sm
                transition-all duration-150 ease-in-out
                transform hover:scale-105 active:scale-95
                focus:outline-none focus:ring-2 focus:ring-offset-2
                disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100
                ${!isConnected 
                  ? 'bg-gray-400'
                  : isSimulating
                    ? 'bg-red-500 hover:bg-red-600 focus:ring-red-500'
                    : 'bg-green-500 hover:bg-green-600 focus:ring-green-500'
                }
              `}
            >
              <div className="flex items-center space-x-2">
                <span className={`h-2 w-2 rounded-full ${isSimulating ? 'bg-red-200' : 'bg-green-200'} animate-pulse`}></span>
                <span>{isSimulating ? 'Stop Simulation' : 'Start Simulation'}</span>
              </div>
            </button>

            <button
              onClick={clearStats}
              className="
                px-6 py-2.5 rounded-lg font-medium text-white
                bg-blue-500 hover:bg-blue-600
                shadow-sm transition-all duration-150 ease-in-out
                transform hover:scale-105 active:scale-95
                focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500
              "
            >
              <div className="flex items-center space-x-2">
                <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                <span>Clear Stats</span>
              </div>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ControlPanel; 