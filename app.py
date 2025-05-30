import os
from datetime import datetime
import io  # for in-memory file
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, session, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from werkzeug.security import generate_password_hash, check_password_hash
# Custom modules
from Config import engine_norms, engine_eres
from Main import process_data
from Utils.sql_queries import get_stock_query, get_job_work, get_sys_output, get_sys_con, get_sys_bipro,get_maxdate

# Define the finished goods list
fg_names = [   
    '2-CHLORO-4-FLUORO-5-NITROPHENYL ETHYL CARBONATE',
    'TPA',
    '1,2 DIMETHYL PROPYL AMINE', 
    '4-FLUORO-3-TRIFLUOROMETHYL PHENOL',
    'M-DEA',
    'M-MEA',
    '2,3 DI CHLORO PYRIDINE',
    'N,N DI ISO PROPYL ETHYL AMINE',   
    '2,5 DIMETHYL PHENYL ACETYL CHLORIDE',   
    'AMIDO CHLORIDE',
    '2-METHOXY BENZOIC ACID',  
    '2,4 DICHLORO BENZOYL CHLORIDE',
    'C-5 HYDROXY ESTER',
    '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE',   
    'DMA-CHLORIDE LAN',   
    '2,6 DIMETHOXY BENZOIC ACID',
    'METHYL-2-CHLORO PROPIONATE',
    '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE',
    '2,4 DICHLORO BENZALDEHYDE',
    '2,6 DICHLORO BENZOYL CHLORIDE',
    'METCAMIFEN TECH.',
    'DICHLORO ACETIC ACID',
]

os.makedirs('Log', exist_ok=True)

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Needed to use sessions

# Configure SQLite database for user authentication
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# User model for SQLite
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), nullable=False, unique=True)
    password = db.Column(db.String(100), nullable=False)
    role = db.Column(db.String(50), nullable=False)

with app.app_context():
    db.create_all()

# Superadmin hardcoded data
superadmin = {
    'username': 'superadmin',
    'password': 'superadmin',
    'role': 'SuperAdmin'
}

def log_user_activity(username, action, fg_name=None):
    """Log user activity to a text file."""
    log_file = os.path.join('Log', 'user_activity_log.txt')
    current_time = datetime.now().replace(tzinfo=None)
    if action == 'logout' and 'login_time' in session:
        login_time = session.pop('login_time')
        session_duration = current_time - login_time.replace(tzinfo=None)
        log_entry = f"{current_time:%Y-%m-%d %H:%M:%S} - {username} {action}. Session Duration: {session_duration}"
    else:
        log_entry = f"{current_time:%Y-%m-%d %H:%M:%S} - {username} {action}"
    if fg_name:
        log_entry += f" | FG_Name: {fg_name}"
    log_entry += "\n"
    with open(log_file, 'a') as f:
        f.write(log_entry)

# -------------------- Helper Functions for Parameterized SQL Queries --------------------
def get_sys_output_filtered(from_date, to_date, fg_name):
    """
    Return the sys_output query with bind parameters.
    Expects from_date and to_date as integer values (YYYYMMDD).
    """
    return get_sys_output(from_date, to_date, fg_name).bindparams(
        from_date=from_date,
        to_date=to_date,
        fg_name=fg_name
    )

def get_sys_con_filtered(from_date, to_date):
    """
    Return the sys_con query (no parameters passed, as get_sys_con takes no arguments).
    """
    base_query = get_sys_con()  # get_sys_con() takes no parameters
    return base_query

def get_sys_bipro_filtered(from_date, to_date, fg_name):
    """
    Return the sys_bipro query with bind parameters.
    Expects from_date and to_date as integer values (YYYYMMDD).
    """
    return get_sys_bipro(from_date, to_date, fg_name).bindparams(
        from_date=from_date,
        to_date=to_date,
        fg_name=fg_name
    )

def get_output_df_and_max_date(from_date, to_date, fg_name, max_retries=3):
    """
    Return the output dataframe and the maximum Output_Voucher_Date.
    Retries up to `max_retries` times on DBAPIError with no delay between attempts.
    """
    # 1) Normalize inputs to YYYYMMDD integers
    if isinstance(from_date, int):
        from_date_int = from_date
    else:
        from_date_int = int(pd.to_datetime(from_date).strftime('%Y%m%d'))
    if isinstance(to_date, int):
        to_date_int = to_date
    else:
        to_date_int = int(pd.to_datetime(to_date).strftime('%Y%m%d'))

    # 2) Get the parametrized SQLAlchemy query
    query = get_sys_output_filtered(from_date_int, to_date_int, fg_name)

    # 3) Immediate-retry loop
    for attempt in range(1, max_retries + 1):
        try:
            df = pd.read_sql(
                query,
                engine_eres,
                params={
                    'from_date': from_date_int,
                    'to_date':   to_date_int,
                    'fg_name':   fg_name
                }
            )
            break
        except DBAPIError:
            if attempt == max_retries:
                # Last attempt failed, re-raise to let caller handle/log it
                raise
            # else: swallow and immediately retry

    # 4) Post-process the date column
    df['Output_Voucher_Date'] = pd.to_datetime(
        df['Output_Voucher_Date'],
        format='%d-%m-%Y',
        errors='coerce'
    )
    max_date = df['Output_Voucher_Date'].max()

    return df, max_date

def get_output_df_and_max_date_default():
    """
    Default for GET requests: use a default fiscal_date and the first finished goods (FG).
    Converts the default dates to integers in YYYYMMDD format.
    """
    fiscal_date = '2024-04-01'
    default_fg = fg_names[0]
    fiscal_date_int = int(pd.to_datetime(fiscal_date).strftime('%Y%m%d'))
    today_int = int(pd.to_datetime(datetime.now()).strftime('%Y%m%d'))
    df, max_date = get_output_df_and_max_date(fiscal_date_int, today_int, default_fg)
    return df, max_date

def process_request_dates(fiscal_date):
    """
    Retrieve start/end dates and FG name from the POST request.
    Uses fiscal_date as default if no start date is provided.
    If end_date is missing, calculates it from output data.
    """
    start_date = request.form.get('start_date') or fiscal_date
    end_date = request.form.get('end_date')
    fg_name = request.form.get('fg_name')
    print("Received start_date and end_date from form:", start_date, end_date)
    if not end_date:
        _, max_date = get_output_df_and_max_date(start_date, datetime.now().strftime('%Y-%m-%d'), fg_name)
        end_date = max_date.strftime('%Y-%m-%d')
    log_user_activity(session['username'], 'searched', fg_name=fg_name)
    return start_date, end_date, fg_name

def render_index(data, start_date, end_date, is_admin, fg_name):
    return render_template(
        'index.html',
        tables=data['Con_qty'].to_dict(orient='records'),
        titles=data['Con_qty'].columns.values if not data['Con_qty'].empty else [],
        fg_names=fg_names,
        output_df=data['output_df'],
        batch_range=data['batch_range'],
        stock_summary=data['stock_summary'],
        bom_summaries_df=data['bom_summaries_df'],
        fg_name_to_items=data['fg_name_to_items'],
        highlighted_items=data['highlighted_items'],
        start_date=start_date,
        end_date=end_date,
        is_admin=is_admin,
        fg_name=fg_name     # <-- forward the real one here
    )

# -------------------- Common Excel Report Generator Function --------------------
def generate_excel_report(data, start_date, end_date, fg_name):
    """
    Create an Excel workbook (a single sheet) using the filtered data.
    Returns a BytesIO object and the filename.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Report")
        
        # --- Define Formats ---
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 20,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#4F46E5',   # Indigo
            'font_color': 'white'
        })
        subheader_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'align': 'left'
        })
        table_header_format = workbook.add_format({
            'bold': True,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#E5E7EB'    # Light gray
        })
        stock_card_header_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#93C5FD',   # Light blue
            'border': 1
        })
        bom_title_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FDE68A',   # Amber
            'border': 1
        })
        bom_header_format = workbook.add_format({
            'bold': True,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FDE68A'
        })
        text_format = workbook.add_format({
            'border': 1,
            'align': 'left',
            'valign': 'vcenter'
        })
        number_format = workbook.add_format({
            'border': 1,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '#,##0.00'
        })
        
        row = 0
        
        # --- Report Header ---
        worksheet.merge_range(row, 0, row, 7, f"Norms Calculator Report - {fg_name}", header_format)
        row += 2
        worksheet.write(row, 0, f"Date Range: {start_date} to {end_date}", subheader_format)
        row += 2
        
        # --- Consumption Norms Table (Horizontal) ---
        if data.get('Con_qty') is not None and not data['Con_qty'].empty:
            df_con = data['Con_qty']
            worksheet.write(row, 0, "Consumption Norms", subheader_format)
            row += 1
            for col_num, col_name in enumerate(df_con.columns):
                worksheet.write(row, col_num, col_name, table_header_format)
            row += 1
            for _, r in df_con.iterrows():
                for col_num, value in enumerate(r):
                    if isinstance(value, (int, float)):
                        worksheet.write(row, col_num, value, number_format)
                    else:
                        worksheet.write(row, col_num, value, text_format)
                row += 1
            row += 2
        
        # --- WIP IN STOCK Section (Vertical Layout) ---
        if data.get('stock_summary'):
            worksheet.merge_range(row, 0, row, 3, "WIP IN STOCK", subheader_format)
            row += 1
            for item in data['stock_summary']:
                stock_title = f"{item.get('Item Name', 'Unknown')} - WIP IN STOCK"
                worksheet.merge_range(row, 0, row, 3, stock_title, stock_card_header_format)
                row += 1
                
                vertical_rows = [
                    ("Opening", item.get("Opening", ""), "", ""),
                    ("WIP PRODUCED", item.get("WIP PRODUCED", ""), "", ""),
                    ("WIP CONSUMED IN PRODUCTION", item.get("WIP CONSUMED IN PRODUCTION", ""), "", ""),
                    ("Closing", item.get("Closing", ""), item.get("Closing_activation", ""), item.get("Closing_mult", "")),
                    ("Quantities Consumed from Opening Stock", item.get("Quantities Consumed from Opening Stock", ""),
                     item.get("Quantities Consumed from Opening Stock_activation", ""), item.get("Quantities_Consumed_mult", "")),
                    ("ADDITIONAL QTY CONSUMED IN OTHER WIP", item.get("ADDITIONAL QTY CONSUMED IN OTHER WIP", ""),
                     item.get("ADDITIONAL QTY CONSUMED IN OTHER WIP_activation", ""), item.get("Additional_QTY_mult", "")),
                    ("NET QTY", "", "", item.get("NET QTY", ""))
                ]
                for vr in vertical_rows:
                    worksheet.write(row, 0, vr[0], table_header_format)
                    if isinstance(vr[1], (int, float)):
                        worksheet.write(row, 1, vr[1], number_format)
                    else:
                        worksheet.write(row, 1, vr[1], text_format)
                    worksheet.write(row, 2, vr[2], text_format)
                    if isinstance(vr[3], (int, float)):
                        worksheet.write(row, 3, vr[3], number_format)
                    else:
                        worksheet.write(row, 3, vr[3], text_format)
                    row += 1
                row += 1  # Blank line after each WIP card

                # --- RM CONSUMPTION IN THE WIP (BOM Summary) ---
                bom_items = [
                    bom for bom in data.get('bom_summaries_df', [])
                    if str(bom.get("Stage Name", "")).strip() == str(item.get("Item Name", "")).strip()
                ]
                if bom_items:
                    bom_title = f"{item.get('Item Name', 'Unknown')} - RM CONSUMPTION IN THE WIP"
                    worksheet.merge_range(row, 0, row, 3, bom_title, bom_title_format)
                    row += 1
                    bom_headers = ["RM Name", "Input Qty", "BOM Qty", "Calc. RM WIP QTY"]
                    for col_num, header in enumerate(bom_headers):
                        worksheet.write(row, col_num, header, bom_header_format)
                    row += 1
                    bom_mapping = {
                        "RM Name": "Name",
                        "Input Qty": "Quantity",
                        "BOM Qty": "BOMQty",
                        "Calc. RM WIP QTY": "RM WIP QTY"
                    }
                    for bom_item in bom_items:
                        for col_num, header in enumerate(bom_headers):
                            key = bom_mapping.get(header, header)
                            value = bom_item.get(key, "")
                            if isinstance(value, (int, float)):
                                worksheet.write(row, col_num, value, number_format)
                            else:
                                worksheet.write(row, col_num, value, text_format)
                        row += 1
                    row += 2
                else:
                    worksheet.merge_range(row, 0, row, 3, "No BOM data available", text_format)
                    row += 2
        
        # Adjust column widths
        worksheet.set_column(0, 0, 45)
        worksheet.set_column(1, 1, 20)
        worksheet.set_column(2, 2, 15)
        worksheet.set_column(3, 3, 25)
    
    output.seek(0)
    filename = f"report_{fg_name}_{start_date}_{end_date}.xlsx"
    return output, filename

@app.route('/admin_redirect', methods=['GET', 'POST'])
def admin_redirect():
    if 'username' not in session or session.get('role') != 'admin':
        return redirect(url_for('login'))

    # … your default-date logic stays the same …
    default_start = '2024-04-01'
    df_max = pd.read_sql(get_maxdate(), engine_eres)
    raw_max = df_max.iloc[0,0]
    max_date = pd.to_datetime(raw_max, errors='coerce').date() or datetime.today().date()
    default_end = max_date.strftime('%Y-%m-%d')
    default_fg  = ''

    # --- DOWNLOAD (GET + ?download=1) ---
    if request.args.get('download'):
        start_date = request.args.get('start_date') or default_start
        end_date   = request.args.get('end_date')   or default_end
        fg_name    = request.args.get('fg_name')    or default_fg

        data = process_data(start_date, end_date, fg_name)
        output, filename = generate_excel_report(data, start_date, end_date, fg_name)
        return send_file( output,
                          download_name=filename,
                          as_attachment=True,
                          mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        )

    # --- FILTER SUBMIT (POST) ---
    if request.method == 'POST':
        start_date, end_date, fg_name = process_request_dates(fiscal_date='2024-04-01')
        data = process_data(start_date, end_date, fg_name)
        # now pass fg_name into render_index
        return render_index(data, start_date, end_date, is_admin=True, fg_name=fg_name)

    # --- INITIAL GET (no download, no POST) ---
    return render_template(
        'index.html',
        tables=[],
        titles=[],
        fg_names=fg_names,
        output_df=None,
        batch_range=None,
        stock_summary=None,
        bom_summaries_df=None,
        fg_name_to_items={},
        highlighted_items=[],
        start_date=default_start,
        end_date=default_end,
        is_admin=True,
        fg_name=default_fg
    )



@app.route('/user_redirect', methods=['GET', 'POST'])
def user_redirect():
    if 'username' not in session or session.get('role') != 'user':
        return redirect(url_for('login'))

    # —— default date logic (same as admin_redirect) ——
    default_start = '2024-04-01'
    # fetch the maximum Output_Voucher_Date from your DB
    df_max    = pd.read_sql(get_maxdate(), engine_eres)
    raw_max   = df_max.iloc[0, 0]
    # coerce to date, fall back to today if NaT
    max_date  = pd.to_datetime(raw_max, errors='coerce').date() or datetime.today().date()
    default_end = max_date.strftime('%Y-%m-%d')
    default_fg  = fg_names[0]

    # —— DOWNLOAD via GET ?download=1 ——
    download_flag = request.args.get('download')
    param_start   = request.args.get('start_date')
    param_end     = request.args.get('end_date')
    param_fg      = request.args.get('fg_name')

    if download_flag:
        # use provided params or fall back to defaults
        start_date = param_start or default_start
        end_date   = param_end   or default_end
        fg_name    = param_fg    or default_fg

        data = process_data(start_date, end_date, fg_name)
        # strip out Rate/Value and “Total” for normal users
        Con_qty = data['Con_qty'].drop(columns=['Rate', 'Value'], errors='ignore')
        Con_qty = Con_qty[Con_qty['Consume_Item_Name'] != 'Total']
        data['Con_qty'] = Con_qty

        output, filename = generate_excel_report(data, start_date, end_date, fg_name)
        return send_file(
            output,
            download_name=filename,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    # —— FORM SUBMIT ——
    if request.method == 'POST':
        # this will also compute end_date via get_output_df_and_max_date if empty
        start_date, end_date, fg_name = process_request_dates(fiscal_date=default_start)
        data = process_data(start_date, end_date, fg_name)

        Con_qty = data['Con_qty'].drop(columns=['Rate', 'Value'], errors='ignore')
        Con_qty = Con_qty[Con_qty['Consume_Item_Name'] != 'Total']
        data['Con_qty'] = Con_qty

        return render_template(
            'index.html',
            tables=data['Con_qty'].to_dict(orient='records'),
            titles=data['Con_qty'].columns.values if not data['Con_qty'].empty else [],
            fg_names=fg_names,
            output_df=data['output_df'],
            batch_range=data['batch_range'],
            stock_summary=data['stock_summary'],
            bom_summaries_df=data['bom_summaries_df'],
            fg_name_to_items=data['fg_name_to_items'],
            highlighted_items=data['highlighted_items'],
            start_date=start_date,
            end_date=end_date,
            is_admin=False,
            fg_name=fg_name
        )

    # —— INITIAL GET ——
    # no POST, no download → show defaults
    data = process_data(default_start, default_end, default_fg)
    Con_qty = data['Con_qty'].drop(columns=['Rate', 'Value'], errors='ignore')
    Con_qty = Con_qty[Con_qty['Consume_Item_Name'] != 'Total']
    data['Con_qty'] = Con_qty

    return render_template(
        'index.html',
        tables=data['Con_qty'].to_dict(orient='records'),
        titles=data['Con_qty'].columns.values if not data['Con_qty'].empty else [],
        fg_names=fg_names,
        output_df=data['output_df'],
        batch_range=data['batch_range'],
        stock_summary=data['stock_summary'],
        bom_summaries_df=data['bom_summaries_df'],
        fg_name_to_items=data['fg_name_to_items'],
        highlighted_items=data['highlighted_items'],
        start_date=default_start,
        end_date=default_end,
        is_admin=False,
        fg_name=default_fg
    )


# -------------------- Routes for Login, Logout, and User Management (unchanged) --------------------
@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].lower()
        password = request.form['password'].lower()
        if username == superadmin['username'] and password == superadmin['password']:
            session['username'] = superadmin['username']
            session['role'] = superadmin['role']
            session['login_time'] = datetime.now()
            log_user_activity(username, 'login')
            return redirect(url_for('admin_panel'))
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            session['username'] = user.username
            session['role'] = user.role
            session['login_time'] = datetime.now()
            log_user_activity(username, 'login')
            if user.role == 'admin':
                return redirect(url_for('admin_redirect'))
            elif user.role == 'user':
                return redirect(url_for('user_redirect'))
        return "Invalid Credentials!"
    return render_template('login.html')
    
@app.route('/logout')
def logout():
    username = session.get('username', 'Unknown User')
    log_user_activity(username, 'logout')
    session.pop('username', None)
    session.pop('role', None)
    return redirect(url_for('login'))

@app.route('/admin')
def admin_panel():
    if 'username' in session and session.get('role') == 'SuperAdmin':
        return render_template('admin_panel.html')
    return redirect(url_for('login'))

@app.route('/add_user', methods=['GET', 'POST'])
def add_user():
    if 'username' in session and session.get('role') == 'SuperAdmin':
        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']
            role = request.form['role']
            hashed_password = generate_password_hash(password, method='sha256')
            new_user = User(username=username, password=hashed_password, role=role)
            db.session.add(new_user)
            db.session.commit()
            return redirect(url_for('user_redirect'))
        return render_template('add_user.html')
    return redirect(url_for('login'))

@app.route('/edit_user/<int:id>', methods=['GET', 'POST'])
def edit_user(id):
    if 'username' in session and session.get('role') == 'SuperAdmin':
        user = User.query.get_or_404(id)
        if request.method == 'POST':
            user.username = request.form['username']
            new_password = request.form['password']
            if new_password:
                user.password = generate_password_hash(new_password, method='sha256')
            user.role = request.form['role']
            db.session.commit()
            return redirect(url_for('user_list'))
        return render_template('edit_user.html', user=user)
    return redirect(url_for('login'))

@app.route('/user_list')
def user_list():
    if 'username' in session and session.get('role') == 'SuperAdmin':
        users = User.query.all()
        return render_template('user_list.html', users=users)
    return redirect(url_for('login'))

@app.route('/delete_user/<int:id>')
def delete_user(id):
    if 'username' in session and session.get('role') == 'SuperAdmin':
        user = User.query.get_or_404(id)
        db.session.delete(user)
        db.session.commit()
        return redirect(url_for('user_list'))
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(debug=True, port=59,host='192.168.1.253')