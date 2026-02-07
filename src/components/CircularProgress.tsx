import React from 'react';

interface CircularProgressProps {
  percentage: number;
  size?: number;
  strokeWidth?: number;
  color?: string;
  label?: string;
}

const CircularProgress: React.FC<CircularProgressProps> = ({
  percentage,
  size = 40,
  strokeWidth = 4,
  color = '#085690',
  label
}) => {
  const radius = (size - strokeWidth) / 2;
  const circumference = radius * 2 * Math.PI;
  const offset = circumference - (percentage / 100) * circumference;

  return (
    <div className="relative inline-flex flex-col items-center justify-center">
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        className="transform -rotate-90"
      >
        {/* Background circle */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke="currentColor"
          strokeWidth={strokeWidth}
          fill="transparent"
          className="text-gray-200"
        />
        {/* Progress circle */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke="currentColor"
          strokeWidth={strokeWidth}
          fill="transparent"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          className="transition-all duration-300 ease-in-out"
          style={{
            stroke: color,
            strokeDashoffset: offset,
          }}
        />
        {/* Inner highlight circle for double-ringed effect */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius - strokeWidth}
          stroke="currentColor"
          strokeWidth={strokeWidth / 2}
          fill="transparent"
          className="text-gray-100"
          opacity="0.5"
        />
      </svg>
      {label && (
        <span className="absolute text-xs font-medium text-gray-700">
          {label}
        </span>
      )}
    </div>
  );
};

export default CircularProgress;