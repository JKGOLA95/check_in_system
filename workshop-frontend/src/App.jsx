import React, { useEffect, useRef, useState } from "react";
import {
  Users,
  Upload,
  Camera,
  BarChart3,
  UserPlus,
  CheckCircle2,
  AlertCircle,
} from "lucide-react";
import "./App.css";

// Configure these for your environment
const API_BASE_URL = "http://localhost:8000/api";
const AUTH_TOKEN = "your-jwt-token-here"; // replace or inject via env

const App = () => {
  // UI state
  const [activeTab, setActiveTab] = useState("dashboard");
  const [isLoading, setIsLoading] = useState(false);
  const [notification, setNotification] = useState(null);

  // Dashboard state
  const [dashboardData, setDashboardData] = useState(null);

  // Registration form state
  const [formData, setFormData] = useState({
    name: "",
    email: "",
    mobile: "",
    batch: "",
  });

  // CSV state
  const [csvFile, setCsvFile] = useState(null);
  const [csvPreview, setCsvPreview] = useState([]);

  // Scanner state
  const [scannerActive, setScannerActive] = useState(false);
  const [lastScan, setLastScan] = useState(null);
  const scannerRef = useRef(null); // holds Html5QrcodeScanner instance

  // Helpers
  const showNotification = (message, type = "success") => {
    setNotification({ message, type });
    window.setTimeout(() => setNotification(null), 4000);
  };

  // API call helper (adds Authorization header only if you set AUTH_TOKEN)
  const apiCall = async (endpoint, options = {}) => {
    const headers = {
      "Content-Type": "application/json",
      ...(AUTH_TOKEN ? { Authorization: `Bearer ${AUTH_TOKEN}` } : {}),
      ...(options.headers || {}),
    };

    const resp = await fetch(`${API_BASE_URL}${endpoint}`, { ...options, headers });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`HTTP ${resp.status} ${resp.statusText} ${text}`.trim());
    }
    const ct = resp.headers.get("content-type") || "";
    return ct.includes("application/json") ? resp.json() : null;
  };

  // Dashboard loader
  const loadDashboard = async () => {
    try {
      setIsLoading(true);
      const data = await apiCall("/attendance/dashboard");
      setDashboardData(data);
    } catch (e) {
      console.error(e);
      setDashboardData(null);
      showNotification("Failed to load dashboard", "error");
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    if (activeTab === "dashboard") loadDashboard();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab]);

  // Form handlers
  const handleInputChange = (field, value) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
  };

  const resetForm = () =>
    setFormData({ name: "", email: "", mobile: "", batch: "" });

  const handleRegistration = async () => {
    const { name, email, mobile, batch } = formData;
    if (!name || !email || !mobile || !batch) {
      showNotification("Please fill in all fields", "error");
      return;
    }

    setIsLoading(true);
    try {
      await apiCall("/register/single", {
        method: "POST",
        body: JSON.stringify(formData),
      });
      showNotification(`${name} registered successfully! QR dispatch queued.`);
      resetForm();
      await loadDashboard();
    } catch (e) {
      console.error(e);
      showNotification("Registration failed. Please check backend logs.", "error");
    } finally {
      setIsLoading(false);
    }
  };

  // CSV handlers
  const handleCsvUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setCsvFile(file);
    try {
      const text = await file.text();
      const rows = text
        .split(/\r?\n/)
        .map((row) => row.split(",").map((cell) => cell.trim()))
        .filter((row) => row.length > 1 && row.some((cell) => cell !== ""));

      setCsvPreview(rows.slice(0, 4)); // header + first 3 rows
    } catch (err) {
      console.error(err);
      showNotification("Error reading CSV file", "error");
      setCsvFile(null);
      setCsvPreview([]);
    }
  };

  const processCsv = async () => {
    if (!csvFile) return;

    setIsLoading(true);
    const form = new FormData();
    form.append("file", csvFile);

    try {
      const response = await fetch(`${API_BASE_URL}/upload/csv`, {
        method: "POST",
        headers: AUTH_TOKEN ? { Authorization: `Bearer ${AUTH_TOKEN}` } : {},
        body: form,
      });

      if (!response.ok) throw new Error("Upload failed");
      const result = await response.json();
      const processed = result.total_processed ?? Math.max(0, csvPreview.length - 1);
      showNotification(`${processed} attendees processed successfully!`);
      setCsvFile(null);
      setCsvPreview([]);
      await loadDashboard();
    } catch (err) {
      console.error(err);
      showNotification("CSV processing failed. Please check format.", "error");
    } finally {
      setIsLoading(false);
    }
  };

  // ---- QR Scanner (with live camera frame) ----
  // Uses html5-qrcode if available. Install: `npm i html5-qrcode`
  const startScanner = async () => {
    setScannerActive(true);
    // Wait for the #qr-reader box to exist in the DOM
    setTimeout(async () => {
      try {
        const mod = await import("html5-qrcode");
        const Html5QrcodeScanner = mod.Html5QrcodeScanner;
        if (!Html5QrcodeScanner) {
          showNotification("Scanner lib not installed. Showing placeholder.", "info");
          return;
        }

        // Create the scanner instance and render into #qr-reader
        const scanner = new Html5QrcodeScanner("qr-reader", { fps: 10, qrbox: 250 }, false);
        scanner.render(
          async (decodedText) => {
            try {
              await handleQrScan(decodedText);
            } finally {
              // stop scanning after a successful read
              try { await scanner.clear(); } catch {}
              scannerRef.current = null;
              setScannerActive(false);
            }
          },
          // per-frame errors (ignore)
          () => {}
        );

        scannerRef.current = scanner;
      } catch (err) {
        console.error(err);
        showNotification("Camera access denied or scanner init failed.", "error");
        setScannerActive(false);
      }
    }, 0);
  };

  const stopScanner = async () => {
    try {
      if (scannerRef.current?.clear) await scannerRef.current.clear();
    } catch {}
    scannerRef.current = null;
    setScannerActive(false);
  };

  const handleQrScan = async (qrData) => {
    try {
      const response = await apiCall("/scan", {
        method: "POST",
        body: JSON.stringify({ qr_code: qrData }),
      });
      const attendee = response?.attendee || {};
      setLastScan({
        name: attendee.name || "",
        email: attendee.email || "",
        batch: attendee.batch || "",
        time: attendee.entry_time ? new Date(attendee.entry_time).toLocaleString() : "",
      });
      showNotification(`✅ ${attendee.name || "Attendee"} - Attendance marked!`);
    } catch (e) {
      console.error(e);
      showNotification("QR code scan failed.", "error");
    }
  };

  const tabs = [
    { id: "dashboard", label: "Dashboard", icon: BarChart3 },
    { id: "register", label: "Register", icon: UserPlus },
    { id: "upload", label: "Bulk Upload", icon: Upload },
    { id: "scanner", label: "Scanner", icon: Camera },
  ];

  return (
    <div className="app-root">
      <header className="app-header">
        <div className="container">
          <h1 className="brand">Workshop Attendance System</h1>
        </div>
      </header>

      {/* Responsive, horizontally scrollable tabs */}
      <div className="container tabs-row">
        <div className="tabs no-scrollbar">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`tab-button ${isActive ? "active" : ""}`}
              >
                <Icon className="icon" />
                <span>{tab.label}</span>
              </button>
            );
          })}
        </div>
      </div>

      <main className="container main-content">
        {activeTab === "dashboard" && (
          <section>
            {isLoading ? (
              <div className="center-block">
                <div className="spinner" />
                <p>Loading dashboard...</p>
              </div>
            ) : dashboardData ? (
              <>
                <div className="grid-3">
                  <div className="card">
                    <div className="card-left">
                      <div className="icon-wrap icon-blue">
                        <Users />
                      </div>
                      <div>
                        <p className="muted">Total Registered</p>
                        <p className="big">{dashboardData.total_attendees ?? "-"}</p>
                      </div>
                    </div>
                  </div>

                  <div className="card">
                    <div className="card-left">
                      <div className="icon-wrap icon-green">
                        <CheckCircle2 />
                      </div>
                      <div>
                        <p className="muted">Attended</p>
                        <p className="big">{dashboardData.marked_attendance ?? "-"}</p>
                      </div>
                    </div>
                  </div>

                  <div className="card">
                    <div className="card-left">
                      <div className="icon-wrap icon-purple">
                        <BarChart3 />
                      </div>
                      <div>
                        <p className="muted">Attendance Rate</p>
                        <p className="big">{dashboardData.attendance_rate ?? "0"}%</p>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="table-card">
                  <div className="table-head">
                    <h3>Batch-wise Attendance</h3>
                  </div>
                  <div className="table-wrap">
                    <table className="att-table">
                      <thead>
                        <tr>
                          <th>Batch</th>
                          <th>Registered</th>
                          <th>Attended</th>
                          <th>Rate</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dashboardData.batch_wise_data?.length ? (
                          dashboardData.batch_wise_data.map((batch, index) => (
                            <tr key={index}>
                              <td>{batch.batch}</td>
                              <td>{batch.total_registered}</td>
                              <td>{batch.total_attended}</td>
                              <td>
                                {batch.total_registered > 0
                                  ? Math.round((batch.total_attended / batch.total_registered) * 100)
                                  : 0}
                                %
                              </td>
                            </tr>
                          ))
                        ) : (
                          <tr>
                            <td colSpan={4} className="muted">
                              No data
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </>
            ) : (
              <div className="center-block">
                <div className="empty-card">
                  <p>Failed to load dashboard data</p>
                  <button className="btn-primary" onClick={loadDashboard}>
                    Retry
                  </button>
                </div>
              </div>
            )}
          </section>
        )}

        {activeTab === "register" && (
          <section className="form-section">
            <div className="card-panel">
              <h2>Register New Attendee</h2>

              <div className="form">
                <label>Full Name</label>
                <input
                  type="text"
                  value={formData.name}
                  onChange={(e) => handleInputChange("name", e.target.value)}
                  placeholder="Enter full name"
                />

                <label>Email Address</label>
                <input
                  type="email"
                  value={formData.email}
                  onChange={(e) => handleInputChange("email", e.target.value)}
                  placeholder="Enter email address"
                />

                <label>Mobile Number</label>
                <input
                  type="tel"
                  value={formData.mobile}
                  onChange={(e) => handleInputChange("mobile", e.target.value)}
                  placeholder="Enter mobile number"
                />

                <label>Batch</label>
                <select
                  value={formData.batch}
                  onChange={(e) => handleInputChange("batch", e.target.value)}
                >
                  <option value="">Select Batch</option>
                  <option value="BATCH_MORNING_01">Morning Batch 1</option>
                  <option value="BATCH_EVENING_01">Evening Batch 1</option>
                  <option value="BATCH_WEEKEND_01">Weekend Batch</option>
                </select>

                <div className="form-actions">
                  <button
                    onClick={handleRegistration}
                    className="btn-primary"
                    disabled={isLoading}
                  >
                    {isLoading ? "Registering..." : "Register & Send QR"}
                  </button>
                  <button onClick={resetForm} className="btn-secondary">
                    Clear
                  </button>
                </div>
              </div>
            </div>
          </section>
        )}

        {activeTab === "upload" && (
          <section>
            <div className="info-box">
              <div className="info-left">
                <AlertCircle />
              </div>
              <div>
                <p className="muted">
                  <strong>CSV Format Required</strong>
                </p>
                <p className="muted">Columns: name, email, mobile, batch</p>
              </div>
            </div>

            <div className="card-panel">
              <h2>Bulk Registration</h2>

              <div className="form">
                <label>Choose CSV File</label>
                <input type="file" accept=".csv" onChange={handleCsvUpload} />

                {csvPreview.length > 0 && (
                  <div className="csv-preview">
                    <h4>Preview (First rows)</h4>
                    <div className="csv-table-wrap">
                      <table className="csv-table">
                        <tbody>
                          {csvPreview.slice(0, 4).map((row, i) => (
                            <tr key={i} className={i === 0 ? "bold-row" : ""}>
                              {row.map((cell, j) => (
                                <td key={j}>{cell}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    <button
                      onClick={processCsv}
                      className="btn-primary"
                      disabled={isLoading}
                    >
                      {isLoading
                        ? "Processing..."
                        : `Process ${Math.max(0, csvPreview.length - 1)} Attendees`}
                    </button>
                  </div>
                )}
              </div>
            </div>
          </section>
        )}

        {activeTab === "scanner" && (
          <section className="form-section">
            <div className="card-panel text-center">
              <h2>QR Code Scanner</h2>

              {!scannerActive ? (
                <>
                  <div className="scanner-icon">
                    <Camera />
                  </div>
                  <p className="muted">Ready to scan QR codes</p>
                  <button onClick={startScanner} className="btn-primary">
                    Start Scanner
                  </button>
                </>
              ) : (
                <>
                  {/* live camera frame renders here when html5-qrcode is installed */}
                  <div id="qr-reader" className="qr-reader-box" />
                  {/* fallback helper text */}
                  <div className="scanner-box">
                    <div className="scanner-target" />
                    <p className="muted-light">Align the QR inside the frame…</p>
                  </div>
                  <button onClick={stopScanner} className="btn-danger">
                    Stop Scanner
                  </button>
                </>
              )}
            </div>

            {lastScan && (
              <div className="card-panel success-card">
                <div className="success-head">
                  <CheckCircle2 />
                  <h3>Last Scanned</h3>
                </div>
                <div className="last-scan">
                  <p>
                    <strong>Name:</strong> {lastScan.name}
                  </p>
                  <p>
                    <strong>Email:</strong> {lastScan.email}
                  </p>
                  <p>
                    <strong>Batch:</strong> {lastScan.batch}
                  </p>
                  <p>
                    <strong>Time:</strong> {lastScan.time}
                  </p>
                </div>
              </div>
            )}
          </section>
        )}
      </main>

      {notification && (
        <div className={`notification ${notification.type}`}>
          <div className="notification-inner">
            {notification.type === "error" && <AlertCircle />}
            {notification.type === "success" && <CheckCircle2 />}
            {notification.type === "info" && <BarChart3 />}
            <span className="notification-text">{notification.message}</span>
          </div>
        </div>
      )}
    </div>
  );
};

export default App;
