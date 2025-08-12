// App.js - Main React Frontend Application
import React, { useState, useEffect } from 'react';
import { Users, Upload, Camera, BarChart3, UserPlus, CheckCircle2 } from 'lucide-react';

const API_BASE_URL = 'http://localhost:8000/api'; // UPDATE WITH YOUR BACKEND URL
const AUTH_TOKEN = 'your-jwt-token-here'; // UPDATE WITH ACTUAL JWT TOKEN

const App = () => {
  const [activeWorkflow, setActiveWorkflow] = useState('dashboard');
  const [isLoading, setIsLoading] = useState(false);
  const [notification, setNotification] = useState(null);

  // Dashboard state
  const [dashboardData, setDashboardData] = useState(null);

  // Single registration state
  const [singleForm, setSingleForm] = useState({
    name: '',
    email: '',
    mobile: '',
    batch: ''
  });

  // Bulk upload state
  const [csvFile, setCsvFile] = useState(null);
  const [csvData, setCsvData] = useState([]);

  // QR Scanner state
  const [scannerActive, setScannerActive] = useState(false);
  const [lastScanResult, setLastScanResult] = useState(null);

  const showNotification = (message, type = 'success') => {
    setNotification({ message, type });
    setTimeout(() => setNotification(null), 5000);
  };

  const apiCall = async (endpoint, options = {}) => {
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`, {
        headers: {
          'Authorization': `Bearer ${AUTH_TOKEN}`,
          'Content-Type': 'application/json',
          ...options.headers,
        },
        ...options,
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      console.error('API call failed:', error);
      showNotification(`Error: ${error.message}`, 'error');
      throw error;
    }
  };

  const loadDashboard = async () => {
    try {
      const data = await apiCall('/attendance/dashboard');
      setDashboardData(data);
    } catch (error) {
      console.error('Failed to load dashboard data');
    }
  };

  useEffect(() => {
    if (activeWorkflow === 'dashboard') {
      loadDashboard();
    }
  }, [activeWorkflow]);

  // Single Registration Handler
  const handleSingleRegistration = async (e) => {
    e.preventDefault();
    setIsLoading(true);
    
    try {
      const response = await apiCall('/register/single', {
        method: 'POST',
        body: JSON.stringify(singleForm),
      });

      showNotification(`${singleForm.name} registered successfully! QR code sent.`);
      setSingleForm({ name: '', email: '', mobile: '', batch: '' });
    } catch (error) {
      showNotification('Registration failed. Please try again.', 'error');
    } finally {
      setIsLoading(false);
    }
  };

  // CSV Upload Handler
  const handleCsvUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setCsvFile(file);
    
    // Parse CSV for preview
    const text = await file.text();
    const rows = text.split('\n').map(row => row.split(','));
    setCsvData(rows.slice(0, 6)); // Show first 5 rows + header
  };

  const processCsvUpload = async () => {
    if (!csvFile) return;
    
    setIsLoading(true);
    const formData = new FormData();
    formData.append('file', csvFile);

    try {
      const response = await fetch(`${API_BASE_URL}/upload/csv`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${AUTH_TOKEN}`,
        },
        body: formData,
      });

      if (!response.ok) throw new Error('Upload failed');
      
      const result = await response.json();
      showNotification(`${result.total_processed} attendees processed successfully!`);
      setCsvFile(null);
      setCsvData([]);
    } catch (error) {
      showNotification('CSV upload failed. Please check format.', 'error');
    } finally {
      setIsLoading(false);
    }
  };

  // QR Scanner Handler
  const startQrScanner = async () => {
    setScannerActive(true);
    try {
      // REQUEST CAMERA PERMISSION AND START SCANNER
      // Note: In a real implementation, you'd use a library like 'qr-scanner' or 'html5-qrcode'
      
      const stream = await navigator.mediaDevices.getUserMedia({ 
        video: { facingMode: 'environment' } 
      });
      
      // This is a placeholder - implement actual QR scanning logic
      showNotification('QR Scanner started. Point camera at QR code.', 'info');
      
      // Simulate QR code detection after 3 seconds (remove in production)
      setTimeout(() => {
        simulateQrScan('WORKSHOP_ATTENDEE:123e4567-e89b-12d3-a456-426614174000');
      }, 3000);
      
    } catch (error) {
      showNotification('Camera access denied or not available.', 'error');
      setScannerActive(false);
    }
  };

  const simulateQrScan = async (qrData) => {
    try {
      const response = await apiCall('/scan', {
        method: 'POST',
        body: JSON.stringify({ qr_code: qrData }),
      });

      setLastScanResult(response);
      showNotification(`✅ ${response.attendee.name} - Attendance marked!`);
      setScannerActive(false);
    } catch (error) {
      showNotification('QR code scan failed.', 'error');
    }
  };

  const stopQrScanner = () => {
    setScannerActive(false);
    // Stop camera stream
  };

  // Workflow Components
  const Dashboard = () => (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-gray-800">Attendance Dashboard</h2>
      
      {dashboardData && (
        <>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="bg-blue-50 p-6 rounded-lg">
              <div className="flex items-center">
                <Users className="w-8 h-8 text-blue-600 mr-3" />
                <div>
                  <p className="text-sm text-blue-600">Total Registered</p>
                  <p className="text-2xl font-bold text-blue-800">{dashboardData.total_attendees}</p>
                </div>
              </div>
            </div>
            
            <div className="bg-green-50 p-6 rounded-lg">
              <div className="flex items-center">
                <CheckCircle2 className="w-8 h-8 text-green-600 mr-3" />
                <div>
                  <p className="text-sm text-green-600">Attendance Marked</p>
                  <p className="text-2xl font-bold text-green-800">{dashboardData.marked_attendance}</p>
                </div>
              </div>
            </div>
            
            <div className="bg-purple-50 p-6 rounded-lg">
              <div className="flex items-center">
                <BarChart3 className="w-8 h-8 text-purple-600 mr-3" />
                <div>
                  <p className="text-sm text-purple-600">Attendance Rate</p>
                  <p className="text-2xl font-bold text-purple-800">{dashboardData.attendance_rate}%</p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white p-6 rounded-lg border">
            <h3 className="text-lg font-semibold mb-4">Batch-wise Attendance</h3>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2">Batch</th>
                    <th className="text-left py-2">Registered</th>
                    <th className="text-left py-2">Attended</th>
                    <th className="text-left py-2">Rate</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.batch_wise_data.map((batch, index) => (
                    <tr key={index} className="border-b">
                      <td className="py-2">{batch.batch}</td>
                      <td className="py-2">{batch.total_registered}</td>
                      <td className="py-2">{batch.total_attended}</td>
                      <td className="py-2">
                        {batch.total_registered > 0 
                          ? Math.round((batch.total_attended / batch.total_registered) * 100)
                          : 0}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );

  const SingleRegistration = () => (
  <div className="space-y-6">
    <h2 className="text-2xl font-bold text-gray-800">Single User Registration</h2>

    <form
      onSubmit={handleSingleRegistration}
      onKeyDown={(e) => { if (e.key === 'Enter') e.preventDefault(); }}  // prevent accidental submits while typing
      className="bg-white p-6 rounded-lg border space-y-4"
    >
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
        <input
          type="text"
          required
          autoComplete="off"
          value={singleForm.name}
          onChange={(e) => setSingleForm(prev => ({ ...prev, name: e.target.value }))}  // functional update
          className="w-full p-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Enter full name"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
        <input
          type="email"
          required
          autoComplete="off"
          value={singleForm.email}
          onChange={(e) => setSingleForm(prev => ({ ...prev, email: e.target.value }))}
          className="w-full p-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Enter email address"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Mobile Number</label>
        <input
          type="tel"
          required
          autoComplete="off"
          inputMode="tel"
          value={singleForm.mobile}
          onChange={(e) => setSingleForm(prev => ({ ...prev, mobile: e.target.value }))}
          className="w-full p-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Enter mobile number"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Batch</label>
        <select
          required
          value={singleForm.batch}
          onChange={(e) => setSingleForm(prev => ({ ...prev, batch: e.target.value }))}
          className="w-full p-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="">Select Batch</option>
          <option value="BATCH_MORNING_01">Morning Batch 1</option>
          <option value="BATCH_EVENING_01">Evening Batch 1</option>
          <option value="BATCH_WEEKEND_01">Weekend Batch</option>
        </select>
      </div>

      <button
        type="submit"
        disabled={isLoading}
        className="w-full bg-blue-600 text-white py-3 px-4 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {isLoading ? 'Registering...' : 'Register & Send QR Code'}
      </button>
    </form>
  </div>
);


  const BulkUpload = () => (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-gray-800">Bulk User Registration</h2>
      
      <div className="bg-blue-50 p-4 rounded-lg">
        <h3 className="font-semibold text-blue-800 mb-2">CSV Format Requirements:</h3>
        <p className="text-blue-700 text-sm">
          Your CSV file must contain columns: <strong>name, email, mobile, batch</strong>
        </p>
      </div>

      <div className="bg-white p-6 rounded-lg border space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">Upload CSV File</label>
          <input
            type="file"
            accept=".csv"
            onChange={handleCsvUpload}
            className="w-full p-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {csvData.length > 0 && (
          <div className="mt-4">
            <h4 className="font-semibold mb-2">Preview (First 5 rows):</h4>
            <div className="overflow-x-auto">
              <table className="w-full text-sm border">
                {csvData.map((row, index) => (
                  <tr key={index} className={index === 0 ? 'bg-gray-100 font-semibold' : ''}>
                    {row.map((cell, cellIndex) => (
                      <td key={cellIndex} className="border p-2">{cell}</td>
                    ))}
                  </tr>
                ))}
              </table>
            </div>
            
            <button
              onClick={processCsvUpload}
              disabled={isLoading}
              className="mt-4 bg-green-600 text-white py-2 px-4 rounded-lg hover:bg-green-700 disabled:opacity-50"
            >
              {isLoading ? 'Processing...' : 'Process CSV & Send QR Codes'}
            </button>
          </div>
        )}
      </div>
    </div>
  );

  const QrScanner = () => (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-gray-800">QR Code Scanner</h2>
      
      <div className="bg-white p-6 rounded-lg border text-center">
        {!scannerActive ? (
          <div className="space-y-4">
            <Camera className="w-16 h-16 text-gray-400 mx-auto" />
            <p className="text-gray-600">Click to start QR code scanner</p>
            <button
              onClick={startQrScanner}
              className="bg-blue-600 text-white py-3 px-6 rounded-lg hover:bg-blue-700"
            >
              Start Camera Scanner
            </button>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="w-full h-64 bg-gray-100 rounded-lg flex items-center justify-center">
              <p className="text-gray-600">📷 Camera View - Point at QR Code</p>
              {/* In production, replace this with actual camera feed */}
            </div>
            <button
              onClick={stopQrScanner}
              className="bg-red-600 text-white py-2 px-4 rounded-lg hover:bg-red-700"
            >
              Stop Scanner
            </button>
          </div>
        )}
      </div>

      {lastScanResult && (
        <div className="bg-green-50 p-6 rounded-lg border border-green-200">
          <h3 className="text-lg font-semibold text-green-800 mb-4">✅ Last Scanned Attendee</h3>
          <div className="space-y-2">
            <p><strong>Name:</strong> {lastScanResult.attendee.name}</p>
            <p><strong>Email:</strong> {lastScanResult.attendee.email}</p>
            <p><strong>Batch:</strong> {lastScanResult.attendee.batch}</p>
            <p><strong>Entry Time:</strong> {new Date(lastScanResult.attendee.entry_time).toLocaleString()}</p>
          </div>
        </div>
      )}
    </div>
  );

  const Navigation = () => (
  <nav className="bg-white border-b border-gray-200 mb-8">
    <div className="max-w-7xl mx-auto px-4">
      <div className="flex space-x-8">
        {[
          { id: 'dashboard', label: 'Dashboard', icon: BarChart3 },
          { id: 'single', label: 'Single Registration', icon: UserPlus },
          { id: 'bulk', label: 'Bulk Upload', icon: Upload },
          { id: 'scanner', label: 'QR Scanner', icon: Camera }
        ].map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            type="button"  // ✅ make it non-submitting
            onClick={() => setActiveWorkflow(id)}
            className={`flex items-center py-4 px-2 border-b-2 font-medium text-sm ${
              activeWorkflow === id
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            <Icon className="w-4 h-4 mr-2" />
            {label}
          </button>
        ))}
      </div>
    </div>
  </nav>
);


  const Notification = () => (
    notification && (
      <div className={`fixed top-4 right-4 p-4 rounded-lg shadow-lg z-50 ${
        notification.type === 'error' ? 'bg-red-100 text-red-700 border border-red-200' :
        notification.type === 'info' ? 'bg-blue-100 text-blue-700 border border-blue-200' :
        'bg-green-100 text-green-700 border border-green-200'
      }`}>
        {notification.message}
      </div>
    )
  );

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <h1 className="text-3xl font-bold text-gray-900">Workshop Attendance Automation</h1>
        </div>
      </header>

      <Navigation />
      
      <main className="max-w-7xl mx-auto px-4 pb-12">
        {activeWorkflow === 'dashboard' && <Dashboard />}
        {activeWorkflow === 'single' && <SingleRegistration />}
        {activeWorkflow === 'bulk' && <BulkUpload />}
        {activeWorkflow === 'scanner' && <QrScanner />}
      </main>

      <Notification />
    </div>
  );
};

export default App;