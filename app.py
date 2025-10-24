# app.py
import os
import io
from datetime import datetime
from io import StringIO 
from types import SimpleNamespace
import re 
import math
import pandas as pd
import xlsxwriter
from bs4 import BeautifulSoup
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, send_file,flash
)
import time
from sqlalchemy.exc import DBAPIError 
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

from Config import engine_norms, engine_eres
from Main import process_data
from Utils.sql_queries import get_sys_output, get_sys_con, get_sys_bipro, get_maxdate
from datetime import timedelta
from datetime import datetime, timezone

# ── local time helper ──────────────────────────────────────────────
from zoneinfo import ZoneInfo          # Python ≥ 3.9
LOCAL_TZ = ZoneInfo("Asia/Kolkata")    # your server’s zone

def now_local_naive() -> datetime:
    """Current IST as a *tz-naive* datetime (easy for text logs)."""
    return datetime.now(LOCAL_TZ).replace(tzinfo=None)


# ──────────────────────────────────────────────────────────────
#  Logging constants – keep them near the other imports
# ──────────────────────────────────────────────────────────────
LOG_DIR  = "Log"
os.makedirs(LOG_DIR, exist_ok=True)                 # make sure folder exists
LOG_FILE = os.path.join(LOG_DIR, "user_activity_log.txt")

def _naive(dt: datetime) -> datetime:
    """Return a tz-naive copy of *dt* (or dt itself if already naive)."""
    if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
        return dt.replace(tzinfo=None)
    return dt

# ── finished-goods master list ─────────────────────────────────────
fg_names = [
    '2-CHLORO-4-FLUORO-5-NITROPHENYL ETHYL CARBONATE', 
    'TPA',
    '4-TERTIARY BUTYL BENZYL CYANIDE',
    '1,2 DIMETHYL PROPYL AMINE', 
    '4-FLUORO-3-TRIFLUOROMETHYL PHENOL',
    'M-DEA', 
    'M-MEA', 
    '2,3 DI CHLORO PYRIDINE',
    'N,N DI ISO PROPYL ETHYL AMINE', 
    '2,5 DIMETHYL PHENYL ACETYL CHLORIDE',
    'AMIDO CHLORIDE', 
    '2-METHOXY BENZOIC ACID',
    '2,4 DICHLORO BENZYL CHLORIDE', 
    'C-5 HYDROXY ESTER',
    '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE',
    'DMA-CHLORIDE LAN', 
    '2,6 DIMETHOXY BENZOIC ACID',
    'METHYL-2-CHLORO PROPIONATE', 
    '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE',
    '2,4 DICHLORO BENZALDEHYDE', 
    '2,6 DICHLORO BENZOYL CHLORIDE',
    'METCAMIFEN TECH.', 
    'DICHLORO ACETIC ACID'
]

# ── Flask and SQLite setup ─────────────────────────────────────────
os.makedirs("Log", exist_ok=True)

app = Flask(__name__)
app.secret_key = "supersecretkey"
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///users.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config.update(
    PERMANENT_SESSION_LIFETIME = timedelta(minutes=50),
    SESSION_REFRESH_EACH_REQUEST = True   # default, but set explicitly for clarity
)
db = SQLAlchemy(app)


class User(db.Model):
    id       = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), nullable=False, unique=True)
    password = db.Column(db.String(100), nullable=False)
    role     = db.Column(db.String(50),  nullable=False)


with app.app_context():
    db.create_all()

superadmin = {"username": "superadmin",
              "password": "superadmin",
              "role":     "SuperAdmin"}

# ── helpers ────────────────────────────────────────────────────────
def log_user_activity(
        username: str,
        action: str,
        fg_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None
) -> None:
    now  = now = now_local_naive()
    line = f"{now:%Y-%m-%d %H:%M:%S} - {username} {action}"

    # add session-duration for normal logout
    if action == "logout" and "login_time" in session:
        duration = now - _naive(session.pop("login_time"))
        line    += f". Session Duration: {duration}"

    # attach FG name and date range when provided
    if fg_name:
        line += f" | FG_Name: {fg_name}"
    if start_date and end_date:
        line += f" | Date_Range: {start_date} → {end_date}"

    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def process_request_dates(fiscal_date):
    start_date = request.form.get("start_date") or fiscal_date
    end_date   = request.form.get("end_date")
    fg_name    = request.form.get("fg_name")
    if not end_date:
        _, mx = get_output_df_and_max_date(start_date,
                                           datetime.now().strftime("%Y-%m-%d"),
                                           fg_name)
        end_date = mx.strftime("%Y-%m-%d")
    log_user_activity(session["username"],"searched",fg_name=fg_name,start_date=start_date,end_date=end_date)
    return start_date, end_date, fg_name


def build_ctx(data, start, end, fg, admin_flag):
    return {
        # table data
        "tables":             data["Con_qty"].to_dict(orient="records"),
        "titles":             data["Con_qty"].columns.values.tolist(),
        # fg list / dropdown
        "fg_names":           fg_names,
        # output-summary / batch-range values
        "output_df":          data["output_df"],
        "batch_from":         data.get("batch_from"),
        "batch_to":           data.get("batch_to"),
        "total_batches":      data.get("total_batches"),
        "batch_range":        data.get("batch_range"),
        # stock / BOM sections
        "stock_summary":      data["stock_summary"],
        "bom_summaries_df":   data["bom_summaries_df"],
        "fg_name_to_items":   data["fg_name_to_items"],
        "highlighted_items":  data["highlighted_items"],
        # misc
        "start_date":         start,
        "end_date":           end,
        "is_admin":           admin_flag,
        "fg_name":            fg,
        "request":            request
        
    }
# ── robust read_sql with automatic pool reset ─────────────────────
def safe_read_sql(query, engine, params=None, tries: int = 3):
    """
    Like pandas.read_sql but retries pool-dispose-&-sleep when a transient
    DBAPIError (‘General network error’, ‘ConnectionWrite’, etc.) occurs.
    """
    for attempt in range(1, tries + 1):
        try:
            return pd.read_sql(query, engine, params=params or {})
        except DBAPIError:
            if attempt == tries:
                raise                              # out of retries → bubble up
            engine.dispose()                       # drop all stale conns
            time.sleep(1)                          # allow SQL Server to settle


# ── HTML→Excel exporter ────────────────────────────────────────────
import io
import re
import math
from datetime import datetime
from types import SimpleNamespace
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup
from flask import render_template

CLR_HDR_BLUE   = "#5158bd"
CLR_HDR_ORANGE = "#f5a44e"
CLR_SECTION_BG = "#e9ecef"
CLR_ALT_ROW_BG = "#f8f9fa"
CLR_BORDER     = "#000000"
CLR_WHITE      = "#ffffff"
DEFAULT_FONT   = "Calibri"
FS_N, FS_M, FS_L, FS_XL = 10, 11, 13, 16

_num_re = re.compile(r'^-?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^-?\d+(?:\.\d+)?$')
is_num_like = lambda v: bool(_num_re.match(v.strip())) if isinstance(v, str) else False


def html_to_excel_with_styles(template: str, ctx: dict,
                              fg_name: str, start: str, end: str):
    fake_req = SimpleNamespace(method="POST", endpoint="dummy",
                               form={"fg_name": fg_name})
    html = render_template(template,
                           **dict(ctx, request=fake_req,
                                  url_for=lambda *_, **__: "#"))

    soup   = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("Rendered template had no <table> elements")

    buf      = io.BytesIO()
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_fg  = re.sub(r"\W+", "_", fg_name or "ALL")
    filename = f"Report_{safe_fg}_{start}_{end}_{ts}.xlsx"

    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        wb = writer.book
        ws = wb.add_worksheet("Report")
        ws.set_default_row(15)

        def mk(**kw):
            base = dict(font_name=DEFAULT_FONT, font_size=FS_N,
                        border=1, border_color=CLR_BORDER,
                        bg_color=CLR_WHITE, align='left', valign='vcenter')
            base.update(kw)
            return wb.add_format(base)

        fmt_title   = mk(bold=1, font_size=FS_XL, align='center',
                         bg_color=CLR_HDR_BLUE, font_color=CLR_WHITE)
        fmt_info_l  = mk(bold=1, align='right')
        fmt_info_v  = mk()
        fmt_section = mk(bold=1, font_size=FS_L, align='center',
                         bg_color=CLR_SECTION_BG)

        fmt_hdr_blue = mk(bold=1, font_size=FS_M, align='center',
                          bg_color=CLR_HDR_BLUE, font_color=CLR_WHITE)
        fmt_hdr_org  = mk(bold=1, font_size=FS_M, align='center',
                          bg_color=CLR_HDR_ORANGE, font_color=CLR_WHITE)

        fmt_left          = mk()
        fmt_left_alt      = mk(bg_color=CLR_ALT_ROW_BG)
        fmt_cent          = mk(align='center')
        fmt_cent_alt      = mk(align='center', bg_color=CLR_ALT_ROW_BG)
        fmt_right_txt     = mk(align='right')
        fmt_right_txt_alt = mk(align='right', bg_color=CLR_ALT_ROW_BG)
        fmt_num           = mk(align='right')
        fmt_num_alt       = mk(align='right', bg_color=CLR_ALT_ROW_BG)
        fmt_dec           = mk(align='right', num_format='#,##0.00')
        fmt_dec_alt       = mk(align='right', num_format='#,##0.00', bg_color=CLR_ALT_ROW_BG)

        r = 0
        ws.merge_range(r, 0, r, 5,
                       "Consumption Norms Report" + (f" – {fg_name}" if fg_name else ""),
                       fmt_title)
        ws.set_row(r, 30); r += 2
        for lbl, val in [("FG Name:", fg_name or "All"),
                         ("Date Range:", f"{start} → {end}"),
                         ("Generated:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]:
            ws.write(r, 0, lbl, fmt_info_l)
            ws.merge_range(r, 1, r, 3, val, fmt_info_v)
            r += 1
        r += 1

        for idx, tbl in enumerate(tables, 1):
            df = pd.read_html(StringIO(str(tbl)), flavor="bs4")[0].fillna('')
            if df.empty:
                continue

            is_bom  = "bom-summary-table" in tbl.get("class", [])
            hdr_fmt = fmt_hdr_org if is_bom else fmt_hdr_blue

            heading = tbl.find_previous(lambda t: t.name in ("h1", "h2", "h3", "h4"))
            title   = heading.get_text(strip=True) if heading else f"Table {idx}"
            ws.merge_range(r, 0, r, len(df.columns)-1, title, fmt_section)
            ws.set_row(r, 24)
            r += 1

            for c, col in enumerate(df.columns):
                ws.write(r, c, str(col), hdr_fmt)
            r += 1

            for ridx, rec in enumerate(df.itertuples(index=False)):
                alt = ridx % 2 == 1
                for c, val in enumerate(rec):
                    if isinstance(val, (int, float)) and math.isfinite(val):
                        fmt = fmt_dec_alt if (alt and isinstance(val, float) and not val.is_integer()) \
                              else fmt_num_alt if (alt and isinstance(val, int)) \
                              else fmt_dec if (isinstance(val, float) and not val.is_integer()) \
                              else fmt_num
                    elif is_num_like(str(val)):
                        fmt = fmt_right_txt_alt if alt else fmt_right_txt
                    else:
                        fmt = (fmt_left_alt if alt else fmt_left) if c == 0 \
                              else (fmt_cent_alt if alt else fmt_cent)
                    ws.write(r, c, val, fmt)
                r += 1

            r += 1

    buf.seek(0)
    return buf, filename

# ── DB helper for max date ─────────────────────────────────────────
def get_sys_output_filtered(frm, to, fg):
    return get_sys_output(frm, to, fg).bindparams(from_date=frm, to_date=to, fg_name=fg)


def get_output_df_and_max_date(from_date, to_date, fg_name, tries=3):
    frm = int(pd.to_datetime(from_date).strftime("%Y%m%d"))
    to  = int(pd.to_datetime(to_date).strftime("%Y%m%d"))
    q   = get_sys_output_filtered(frm, to, fg_name)

    for _ in range(tries):
        df = pd.read_sql(q, engine_eres,
                         params={"from_date": frm, "to_date": to, "fg_name": fg_name})
        if not df.empty:
            break
    df["Output_Voucher_Date"] = pd.to_datetime(df["Output_Voucher_Date"],
                                               format="%d-%m-%Y", errors="coerce")
    return df, df["Output_Voucher_Date"].max()

IDLE_LIMIT = timedelta(minutes=20)   # same as PERMANENT_SESSION_LIFETIME


def safe_process_data(start_date, end_date, fg_name, tries: int = 3):
    for attempt in range(1, tries + 1):
        try:
            return process_data(start_date, end_date, fg_name)
        except DBAPIError:
            if attempt == tries:
                raise
            engine_eres.dispose()
            engine_norms.dispose()
            time.sleep(1)

@app.before_request
def enforce_idle_timeout():
    # Skip if user isn’t logged in
    if "username" not in session:
        return

    now = now_local_naive()
    last_seen = session.get("last_activity")

    # First request in this session → just stamp the time
    if not last_seen:
        session["last_activity"] = now.isoformat()
        return

    # Convert ISO string back to datetime
    last_seen_dt = datetime.fromisoformat(last_seen)

    # Too much idle time?
    if now - last_seen_dt > IDLE_LIMIT:
        # Record the auto-logout in the text log
        log_user_activity(session["username"], "auto-logout")

        # Clear the session and send user to login
        session.clear()
        flash("You were logged out after 20 minutes of inactivity.", "warning")
        return redirect(url_for("login"))

    # Otherwise, refresh the activity timestamp for next request
    session["last_activity"] = now.isoformat()

# ── ADMIN ROUTE (replace whole function) ──────────────────────────
@app.route("/admin_redirect", methods=["GET", "POST"])
def admin_redirect():
    if "username" not in session or session.get("role") != "admin":
        return redirect(url_for("login"))

    default_start = "2025-04-01"
    raw_max       = safe_read_sql(get_maxdate(), engine_eres).iloc[0, 0]
    default_end   = (pd.to_datetime(raw_max, errors="coerce") or datetime.today()).strftime("%Y-%m-%d")

    # ── download link (GET) ───────────────────────────────────────
    if request.args.get("download"):
        st = request.args.get("start_date") or default_start
        ed = request.args.get("end_date")   or default_end
        fg = request.args.get("fg_name")    or ""
        log_user_activity(session["username"], "downloaded_excel",
                          fg_name=fg, start_date=st, end_date=ed)

        data        = safe_process_data(st, ed, fg)
        ctx         = build_ctx(data, st, ed, fg, True)
        buff, fname = html_to_excel_with_styles("index.html", ctx, fg, st, ed)
        return send_file(buff, download_name=fname, as_attachment=True,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ── form submit (POST) ────────────────────────────────────────
    if request.method == "POST":
        st, ed, fg = process_request_dates(default_start)
        data       = safe_process_data(st, ed, fg)
        return render_template("index.html", **build_ctx(data, st, ed, fg, True))

    # ── first page load (GET) ─────────────────────────────────────
    empty = {"Con_qty": pd.DataFrame(),
             "output_df": None,
             "batch_range": None,
             "stock_summary": None,
             "bom_summaries_df": None,
             "fg_name_to_items": {},
             "highlighted_items": []}
    return render_template("index.html",
                           **build_ctx(empty, default_start, default_end, "", True))

# ── user_redirect ─────────────────────────────────────────────────
# ── USER ROUTE (replace whole function) ───────────────────────────
@app.route("/user_redirect", methods=["GET", "POST"])
def user_redirect():
    if "username" not in session or session.get("role") != "user":
        return redirect(url_for("login"))

    default_start = "2024-04-01"
    raw_max       = safe_read_sql(get_maxdate(), engine_eres).iloc[0, 0]
    default_end   = (pd.to_datetime(raw_max, errors="coerce") or datetime.today()).strftime("%Y-%m-%d")
    default_fg    = fg_names[0]

    # ── download link (GET) ───────────────────────────────────────
    if request.args.get("download"):
        st = request.args.get("start_date") or default_start
        ed = request.args.get("end_date")   or default_end
        fg = request.args.get("fg_name")    or default_fg
        log_user_activity(session["username"], "downloaded_excel",
                          fg_name=fg, start_date=st, end_date=ed)

        data = safe_process_data(st, ed, fg)
        # hide Rate/Value for normal users
        con  = data["Con_qty"].drop(columns=["Rate", "Value"], errors="ignore")
        data["Con_qty"] = con[con["Consume_Item_Name"] != "Total"]

        ctx         = build_ctx(data, st, ed, fg, False)
        buff, fname = html_to_excel_with_styles("index.html", ctx, fg, st, ed)
        return send_file(buff, download_name=fname, as_attachment=True,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ── form submit (POST) ────────────────────────────────────────
    if request.method == "POST":
        st, ed, fg = process_request_dates(default_start)
        data       = safe_process_data(st, ed, fg)
        con        = data["Con_qty"].drop(columns=["Rate", "Value"], errors="ignore")
        data["Con_qty"] = con[con["Consume_Item_Name"] != "Total"]
        return render_template("index.html", **build_ctx(data, st, ed, fg, False))

    # ── first page load (GET) ─────────────────────────────────────
    data = safe_process_data(default_start, default_end, default_fg)
    con  = data["Con_qty"].drop(columns=["Rate", "Value"], errors="ignore")
    data["Con_qty"] = con[con["Consume_Item_Name"] != "Total"]
    return render_template("index.html",
                           **build_ctx(data, default_start, default_end,
                                       default_fg, False))

# ── authentication & user-management routes ───────────────────────
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form["username"].lower()
        p = request.form["password"].lower()

        if u == superadmin["username"] and p == superadmin["password"]:
            session.permanent = True
            session.update(username=u, role=superadmin["role"], login_time=now_local_naive())
            log_user_activity(u, "login")
            return redirect(url_for("admin_panel"))

        user = User.query.filter_by(username=u).first()
        if user and check_password_hash(user.password, p):
            session.permanent = True
            session.update(username=user.username, role=user.role, login_time=now_local_naive())
            log_user_activity(u, "login")
            return redirect(url_for("admin_redirect" if user.role == "admin" else "user_redirect"))

        return "Invalid credentials!"
    return render_template("login.html")


@app.route("/logout")
def logout():
    log_user_activity(session.get("username", "Unknown"), "logout")
    session.clear()
    return redirect(url_for("login"))


@app.route("/admin")
def admin_panel():
    if session.get("role") == "SuperAdmin":
        return render_template("admin_panel.html")
    return redirect(url_for("login"))


@app.route("/add_user", methods=["GET", "POST"])
def add_user():
    if session.get("role") != "SuperAdmin":
        return redirect(url_for("login"))
    if request.method == "POST":
        username = request.form["username"]
        password = generate_password_hash(request.form["password"], method="sha256")
        role     = request.form["role"]
        db.session.add(User(username=username, password=password, role=role))
        db.session.commit()
        return redirect(url_for("user_list"))
    return render_template("add_user.html")


@app.route("/edit_user/<int:uid>", methods=["GET", "POST"])
def edit_user(uid):
    if session.get("role") != "SuperAdmin":
        return redirect(url_for("login"))
    user = User.query.get_or_404(uid)
    if request.method == "POST":
        user.username = request.form["username"]
        if request.form["password"]:
            user.password = generate_password_hash(request.form["password"], method="sha256")
        user.role = request.form["role"]
        db.session.commit()
        return redirect(url_for("user_list"))
    return render_template("edit_user.html", user=user)


@app.route("/user_list")
def user_list():
    if session.get("role") != "SuperAdmin":
        return redirect(url_for("login"))
    return render_template("user_list.html", users=User.query.all())


@app.route("/delete_user/<int:uid>")
def delete_user(uid):
    if session.get("role") != "SuperAdmin":
        return redirect(url_for("login"))
    db.session.delete(User.query.get_or_404(uid))
    db.session.commit()
    return redirect(url_for("user_list"))

# ── run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    # app.run(debug=True, port=59)
    app.run(debug=True, port=59,host='192.168.1.253')
