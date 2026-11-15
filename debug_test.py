import app
print("1: imported app", flush=True)

engine = app.CampaignEngine()
print("2: created engine", flush=True)

engine._load_config = lambda: None
engine.min_delay = 1
engine.max_delay = 1
engine._send_sms = lambda p, m, s: (True, "HTTP 202")
engine.contacts = [("+998901111111", "Ali"), ("+998902222222", "Vali")]
engine.message_template = "Test"
engine.total = 2
engine.campaign_id = "test_direct_debug"
engine._pause_event.set()
engine._stop_event.clear()

print("3: starting loop manually", flush=True)
for i, (phone, name) in enumerate(engine.contacts):
    print(f"4: loop i={i}, phone={phone}", flush=True)
    engine._pause_event.wait()
    print("5: pause_event.wait passed", flush=True)
    with engine.lock:
        engine.current_phone = phone
    print("6: lock passed", flush=True)
    sim = engine._get_current_sim()
    print("7: sim passed:", sim, flush=True)
    success, error = engine._send_sms(phone, "Test", sim)
    print("8: send_sms passed:", success, error, flush=True)
    with engine.lock:
        engine.current_phone = ""
        engine.sent += 1
        engine._add_log(phone, "sent", f"✅ Yuborildi: {phone}")
    print("9: lock 2 passed", flush=True)
    with app.get_db() as conn:
        print("10: get_db passed", flush=True)
        conn.execute(
            "INSERT INTO sms_logs (campaign_id, phone, name, message, status, error_msg, sim_used) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (engine.campaign_id, phone, name, "Test", "sent", "", sim),
        )
        print("11: insert passed", flush=True)
        conn.commit()
        print("12: commit passed", flush=True)
    
    print("13: done with contact", phone, flush=True)

print("ALL CONTACTS PASSED!")

