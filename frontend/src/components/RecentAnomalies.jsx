import React from 'react';

const RecentAnomalies = ({ recentAnomalies }) => {
  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  return (
    <div className="px-4 py-6 sm:px-0">
      <h2 className="text-lg font-medium text-gray-900 mb-4">
        Recent Anomalies ({recentAnomalies.length})
      </h2>
      <div className="border-4 border-dashed border-gray-200 rounded-lg p-4">
        {recentAnomalies.length === 0 ? (
          <div className="text-center text-gray-500 py-8">
            No anomalies detected yet
          </div>
        ) : (
          <div className="space-y-4">
            {recentAnomalies.map((anomaly, index) => (
              <div
                key={`${anomaly.timestamp}-${index}`}
                className={`bg-white shadow rounded-lg p-4 hover:shadow-lg transition-shadow ${
                  anomaly.score >= 0.8 ? 'border-l-4 border-red-500' : 'border-l-4 border-yellow-500'
                }`}
              >
                <div className="flex justify-between items-start">
                  <div className="flex-1">
                    <h3 className={`text-card-title ${
                      anomaly.score >= 0.8 ? 'text-red-800' : 'text-yellow-800'
                    }`}>
                      {anomaly.score >= 0.8 ? 'Critical Anomaly' : 'Warning Anomaly'}
                    </h3>
                    <p className="mt-1 text-log-content text-gray-600">
                      {anomaly.text || 'No details available'}
                    </p>
                    <p className="mt-2 text-xs text-gray-500">
                      {formatTimestamp(anomaly.timestamp)}
                    </p>
                    <p className="mt-1 text-xs text-gray-400">
                      Type: {anomaly.type} {anomaly.sub_type ? `(${anomaly.sub_type})` : ''}
                    </p>
                  </div>
                  <div className="ml-4">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      anomaly.score >= 0.8 
                        ? 'bg-red-100 text-red-800' 
                        : 'bg-yellow-100 text-yellow-800'
                    }`}>
                      Score: {anomaly.score.toFixed(2)}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default RecentAnomalies; 