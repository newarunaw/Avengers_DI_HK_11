# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DI_HK11_CUSTOMER_MASTER (
# MAGIC     customer_id VARCHAR(20),
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     email VARCHAR(100),
# MAGIC     phone_number VARCHAR(20),
# MAGIC     pan_number VARCHAR(20),
# MAGIC     created_at TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DI_HK11_CUSTOMER_MASTER VALUES
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C001', 'Ravi', 'Kumar', 'ravi.kumar@example.com', '9876543210', 'ABCDE1234F', '2023-01-01 10:00:00', '2023-02-01 12:00:00'),
# MAGIC  
# MAGIC -- ❌ Missing email
# MAGIC  
# MAGIC ('C002', 'Meena', 'Iyer', NULL, '9876500000', 'PQRSX5678Z', '2023-01-05 09:00:00', '2023-02-02 14:30:00'),
# MAGIC  
# MAGIC -- ❌ Invalid phone number
# MAGIC  
# MAGIC ('C003', 'Arun', 'Sharma', 'arun.sharma@example.com', '98XX543210', 'LMNOP3456Y', '2023-01-10 11:15:00', '2023-02-05 16:00:00'),
# MAGIC  
# MAGIC -- ❌ Duplicate customer_id (C001)
# MAGIC  
# MAGIC ('C001', 'Ravi', 'Kumar', 'ravi.duplicate@example.com', '9123456789', 'ABCDE1234F', '2023-01-01 10:05:00', '2023-02-03 12:00:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C004', 'Sneha', 'Patel', 'sneha.patel@example.com', '9988776655', 'XYZAB6789C', '2023-01-12 08:00:00', '2023-02-04 10:00:00'),
# MAGIC  
# MAGIC -- ❌ Invalid PAN (too short)
# MAGIC  
# MAGIC ('C005', 'Vikas', 'Nair', 'vikas.nair@example.com', '9765432100', 'ABCDE12', '2023-01-14 13:20:00', '2023-02-06 15:30:00'),
# MAGIC  
# MAGIC -- ❌ Missing phone
# MAGIC  
# MAGIC ('C006', 'Kavya', 'Menon', 'kavya.menon@example.com', NULL, 'MNBVC4321P', '2023-01-16 09:45:00', '2023-02-07 11:00:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C007', 'Anil', 'Gupta', 'anil.gupta@example.com', '9090909090', 'TGBHN5678U', '2023-01-18 07:30:00', '2023-02-08 12:30:00'),
# MAGIC  
# MAGIC -- ❌ Invalid email format
# MAGIC  
# MAGIC ('C008', 'Priya', 'Das', 'priya.das#email.com', '9234567890', 'PLMOK9876Q', '2023-01-20 15:00:00', '2023-02-09 09:30:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C009', 'Ramesh', 'Jain', 'ramesh.jain@example.com', '9345678901', 'ASDFG1234H', '2023-01-22 10:10:00', '2023-02-10 08:00:00'),
# MAGIC  
# MAGIC -- ❌ Duplicate customer_id (C002)
# MAGIC  
# MAGIC ('C002', 'Meena', 'Iyer', 'meena.alt@example.com', '9000000000', 'PQRSX5678Z', '2023-01-05 09:15:00', '2023-02-11 10:00:00'),
# MAGIC  
# MAGIC -- ❌ PAN with special chars
# MAGIC  
# MAGIC ('C010', 'Deepak', 'Verma', 'deepak.verma@example.com', '9456789012', 'ABCD#1234!', '2023-01-25 14:30:00', '2023-02-12 14:45:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C011', 'Lakshmi', 'Rao', 'lakshmi.rao@example.com', '9567890123', 'QWERT6789U', '2023-01-26 11:00:00', '2023-02-13 13:00:00'),
# MAGIC  
# MAGIC -- ❌ Null PAN
# MAGIC  
# MAGIC ('C012', 'Suresh', 'Pillai', 'suresh.pillai@example.com', '9678901234', NULL, '2023-01-27 12:00:00', '2023-02-14 12:00:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C013', 'Asha', 'Krishnan', 'asha.krishnan@example.com', '9789012345', 'ZXCVB1234N', '2023-01-28 08:15:00', '2023-02-15 16:20:00'),
# MAGIC  
# MAGIC -- ❌ Invalid phone (too short)
# MAGIC  
# MAGIC ('C014', 'Tarun', 'Bose', 'tarun.bose@example.com', '12345', 'HGFDS4321K', '2023-01-29 09:40:00', '2023-02-16 11:10:00'),
# MAGIC  
# MAGIC -- ❌ Null email + phone
# MAGIC  
# MAGIC ('C015', 'Geeta', 'Mishra', NULL, NULL, 'LKJHG9876M', '2023-01-30 07:55:00', '2023-02-17 10:30:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C016', 'Manoj', 'Yadav', 'manoj.yadav@example.com', '9898989898', 'CVBNM4321L', '2023-02-01 13:00:00', '2023-02-18 12:40:00'),
# MAGIC  
# MAGIC -- ❌ Invalid email (no domain)
# MAGIC  
# MAGIC ('C017', 'Pooja', 'Shah', 'pooja.shah@', '9230000000', 'DFGHJ5678T', '2023-02-02 10:10:00', '2023-02-19 15:30:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C018', 'Nitin', 'Kapoor', 'nitin.kapoor@example.com', '9123450000', 'BNMLK1234R', '2023-02-03 11:11:00', '2023-02-20 12:12:00'),
# MAGIC  
# MAGIC -- ❌ Duplicate PAN (same as C018)
# MAGIC  
# MAGIC ('C019', 'Rohit', 'Agarwal', 'rohit.agarwal@example.com', '9345611111', 'BNMLK1234R', '2023-02-04 09:30:00', '2023-02-21 14:00:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C020', 'Divya', 'Chopra', 'divya.chopra@example.com', '9456123456', 'MKOIJ8765W', '2023-02-05 10:20:00', '2023-02-22 10:40:00'),
# MAGIC  
# MAGIC -- ❌ Invalid PAN (too long)
# MAGIC  
# MAGIC ('C021', 'Sanjay', 'Bhatt', 'sanjay.bhatt@example.com', '9234567899', 'ASDFG1234HJKL', '2023-02-06 12:25:00', '2023-02-23 09:30:00'),
# MAGIC  
# MAGIC -- ❌ Invalid phone (letters inside)
# MAGIC  
# MAGIC ('C022', 'Rekha', 'Singh', 'rekha.singh@example.com', '98AB543210', 'PLKJI9876Y', '2023-02-07 14:15:00', '2023-02-24 13:20:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C023', 'Alok', 'Joshi', 'alok.joshi@example.com', '9345678902', 'QAZWS1234X', '2023-02-08 16:00:00', '2023-02-25 11:00:00'),
# MAGIC  
# MAGIC -- ❌ Null everything except ID
# MAGIC  
# MAGIC ('C024', 'Unknown', 'User', NULL, NULL, NULL, '2023-02-09 10:00:00', '2023-02-26 09:00:00'),
# MAGIC  
# MAGIC -- ✅ Good record
# MAGIC  
# MAGIC ('C025', 'Farah', 'Ali', 'farah.ali@example.com', '9876501234', 'MNBVC7654Z', '2023-02-10 12:45:00', '2023-02-27 15:15:00');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC sELECT * FROM DI_HK11_CUSTOMER_MASTER
# MAGIC