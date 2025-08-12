-- database_schema.sql
-- Workshop Attendance Automation Database Schema

-- Create database (run this separately if needed)
-- CREATE DATABASE workshop_attendance_db;

-- Use the database
-- \c workshop_attendance_db;

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Attendees table
CREATE TABLE attendees (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    mobile VARCHAR(20) NOT NULL,
    batch VARCHAR(100) NOT NULL,
    qr_code TEXT NOT NULL, -- Base64 encoded QR code image
    qr_data VARCHAR(500) NOT NULL UNIQUE, -- QR code data for scanning
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Attendance table
CREATE TABLE attendance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    attendee_id UUID NOT NULL REFERENCES attendees(id) ON DELETE CASCADE,
    entry_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(attendee_id) -- Ensure one attendance record per attendee
);

-- Batches table (optional - for better batch management)
CREATE TABLE batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    max_attendees INTEGER DEFAULT 100,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Users table (for CRM/Sales staff authentication)
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'staff', -- 'admin', 'staff'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP
);

-- Audit log table
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    action VARCHAR(100) NOT NULL, -- 'REGISTER', 'SCAN', 'BULK_UPLOAD'
    details JSONB,
    ip_address INET,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for better performance
CREATE INDEX idx_attendees_email ON attendees(email);
CREATE INDEX idx_attendees_mobile ON attendees(mobile);
CREATE INDEX idx_attendees_batch ON attendees(batch);
CREATE INDEX idx_attendees_qr_data ON attendees(qr_data);
CREATE INDEX idx_attendance_attendee_id ON attendance(attendee_id);
CREATE INDEX idx_attendance_entry_time ON attendance(entry_time);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);
CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at
CREATE TRIGGER update_attendees_updated_at 
    BEFORE UPDATE ON attendees 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Views for reporting
CREATE VIEW attendance_summary AS
SELECT 
    b.name as batch_name,
    COUNT(a.id) as total_registered,
    COUNT(att.id) as total_attended,
    ROUND(
        CASE 
            WHEN COUNT(a.id) > 0 THEN (COUNT(att.id)::DECIMAL / COUNT(a.id)) * 100 
            ELSE 0 
        END, 2
    ) as attendance_percentage
FROM batches b
LEFT JOIN attendees a ON b.name = a.batch
LEFT JOIN attendance att ON a.id = att.attendee_id
GROUP BY b.id, b.name;

-- View for daily attendance report
CREATE VIEW daily_attendance AS
SELECT 
    DATE(att.entry_time) as attendance_date,
    COUNT(*) as total_attendees,
    STRING_AGG(DISTINCT a.batch, ', ') as batches
FROM attendance att
JOIN attendees a ON att.attendee_id = a.id
GROUP BY DATE(att.entry_time)
ORDER BY attendance_date DESC;

-- Sample data for testing (optional)
INSERT INTO batches (name, description, start_date, max_attendees) VALUES
('BATCH_MORNING_01', 'Morning Session Batch 1', NOW() + INTERVAL '1 day', 50),
('BATCH_EVENING_01', 'Evening Session Batch 1', NOW() + INTERVAL '1 day', 50),
('BATCH_WEEKEND_01', 'Weekend Special Batch', NOW() + INTERVAL '2 days', 30);

-- Sample admin user (password: admin123 - hash this in production!)
INSERT INTO users (username, email, password_hash, role) VALUES
('admin', 'admin@workshop.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtABWu7Y3BHNY5k9Qr7YM6K2ZE6', 'admin');

-- Grant permissions (adjust based on your PostgreSQL setup)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO workshop_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO workshop_user;