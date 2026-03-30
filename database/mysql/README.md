# MySQL Setup

โครงสร้างนี้ออกแบบมาสำหรับเก็บประวัติการเช็กระบบ, รายงานราย service และ artifact paths จาก Python checker ตัวเดิม โดยแนะนำให้ใช้ MySQL 8

## ทำไมใช้ MySQL ได้ดีในโปรเจกต์นี้

- เหมาะถ้า infra เดิมของทีมใช้ MySQL อยู่แล้ว
- query ตาม site, host, service, status ได้ตรงไปตรงมา
- รองรับ `JSON` สำหรับเก็บ raw payload ของแต่ละ run/check
- ใช้ร่วมกับ Python worker ได้ง่าย

## เริ่มใช้งาน

1. สร้างฐานข้อมูล `server_checker`
2. รัน schema:

```bash
mysql -u root -p server_checker < database/mysql/001_init.sql
```

3. ตั้ง environment variable:

```bash
export SERVER_CHECKER_DATABASE_URL="mysql://root:password@127.0.0.1:3306/server_checker"
```

4. ติดตั้ง dependency ใหม่:

```bash
pip install -r requirements.txt
```

5. รัน checker ตามเดิม:

```bash
python run.py
```

ถ้ามี `SERVER_CHECKER_DATABASE_URL` ระบบจะเขียนผล run ลง database อัตโนมัติ

## ตารางหลัก

- `check_runs`: เก็บภาพรวมแต่ละรอบที่รัน
- `site_run_reports`: เก็บไฟล์ report ระดับ site
- `service_results`: เก็บผลของแต่ละ service ในแต่ละ run
- `service_check_steps`: เก็บผลของแต่ละ command/check step
- `sites`, `hosts`, `services`: เก็บ master data สำหรับใช้งานในรายงานและการ query

## หมายเหตุ

- schema นี้ไม่เก็บ password จาก `hosts.yaml`
