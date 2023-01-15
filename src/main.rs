use std::fs;
use std::time::Instant;

use rusqlite::{Connection, Result};

fn insert_data(conn: &mut Connection) -> Result<()> {
    let mut t: u64 = 1600000000;
    let num_entries = 1000 * 1000;
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO data(dt, id, label)
        VALUES (?, (SELECT num FROM autoinc), 'hello');",
        )?;
        for i in 0..num_entries {
            if i % (num_entries / 10) == 0 {
                println!("Processing {:?}%", i / (num_entries / 100));
            }
            stmt.execute((t,))?;
            if i % 100 == 0 {
                t += 1;
            }
        }
    }
    tx.commit()
}

fn main() -> Result<()> {
    let db_path = "test.db";
    if std::path::Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }
    let mut conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;

    conn.execute("CREATE TABLE autoinc(num INTEGER)", ())?;
    conn.execute("INSERT INTO autoinc(num) VALUES(0)", ())?;
    conn.execute(
        "CREATE TABLE data(
            dt INTEGER, id INTEGER,
            label TEXT,
            PRIMARY KEY(dt, id)
        ) WITHOUT ROWID",
        (),
    )?;
    conn.execute(
        "CREATE TRIGGER insert_trigger BEFORE INSERT ON data BEGIN UPDATE autoinc SET num=num+1; END;",
        (),
    )?;
    println!("Created table");
    let now = Instant::now();
    insert_data(&mut conn)?;
    println!("Elapsed: {:.2?}", now.elapsed());
    println!("INSERT done");

    let t1: u64 = 1600005000;
    let t2: u64 = 1600005100;
    let now = Instant::now();
    let mut stmt = conn.prepare("SELECT 1 FROM data WHERE dt BETWEEN ? AND ?")?;
    let dt_iter = stmt.query_map((t1, t2), |_row| Ok(()))?;

    let mut i = 0;
    for _ in dt_iter {
        i += 1;
    }
    println!("Elapsed: {:.2?}", now.elapsed());
    println!("Found results {:?}", i);
    Ok(())
}
