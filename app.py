from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import text
from Config import engine_norms, engine_eres
from Main import process_data

app = Flask(__name__)

# # Define the fg_names list
# fg_names = [
#     # '2,3 DI CHLORO PYRIDINE',
#     'N,N DI ISO PROPYL ETHYL AMINE',
#     '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE',
#     '2,5 DIMETHYL PHENYL ACETYL CHLORIDE',
#     # 'DICHLORO ACETIC ACID',
#     'AMIDO CHLORIDE',
#     '2-METHOXY BENZOIC ACID',
#     # 'METCAMIFEN TECH.',
#     '2,4 DICHLORO BENZOYL CHLORIDE',
#     'C-5 HYDROXY ESTER',
#     '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE',
#     # '2,4 DICHLORO BENZALDEHYDE',
#     # '2,6 DICHLORO BENZOYL CHLORIDE',
#     'DMA-CHLORIDE LAN',
#     '4-FLUORO-3-TRIFLUOROMETHYL PHENOL',
#     '2,6 DIMETHOXY BENZOIC ACID'
# ]

@app.route('/rate', methods=['GET', 'POST'])
def index():
    # Fetch FG names for the dropdown
    query_out_product = 'SELECT * FROM Output_Production;'
    df_out_product = pd.read_sql(query_out_product, engine_norms)
    fg_names = df_out_product['FG_Name'].unique()

    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        fg_name = request.form.get('fg_name')

        data = process_data(start_date, end_date, fg_name)

        Con_qty = data['Con_qty']

        tables = [Con_qty.to_html(classes='table table-striped table-bordered', header="true", index=False)]

        return render_template(
            'index.html',
            tables=tables,
            titles=Con_qty.columns.values if not Con_qty.empty else [],
            fg_names=fg_names,
            output_df=data['output_df'],
            batch_range=data['batch_range'],
            stock_summary=data['stock_summary'],
            bom_summaries_df=data['bom_summaries_df'],
            fg_name_to_items=data['fg_name_to_items'],
            highlighted_items=data['highlighted_items']
        )
    else:
        return render_template('index.html', fg_names=fg_names)

@app.route('/', methods=['GET', 'POST'])
def rate():
    # Fetch FG names for the dropdown
    # query_out_product = 'SELECT * FROM Output_Production;'
    # df_out_product = pd.read_sql(query_out_product, engine_norms)
    # fg_names = df_out_product['FG_Name'].unique()

    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        fg_name = request.form.get('fg_name')

        data = process_data(start_date, end_date, fg_name)

        Con_qty = data['Con_qty']

        # Drop 'Rate' and 'Value' columns
        Con_qty = Con_qty.drop(columns=['Rate', 'Value'])
        Con_qty = Con_qty[Con_qty['Consume_Item_Name'] != 'Total']

        tables = [Con_qty.to_html(classes='table table-striped table-bordered', header="true", index=False)]

        return render_template(
            'index.html',
            tables=tables,
            titles=Con_qty.columns.values if not Con_qty.empty else [],
            fg_names=fg_names,
            output_df=data['output_df'],
            batch_range=data['batch_range'],
            stock_summary=data['stock_summary'],
            bom_summaries_df=data['bom_summaries_df'],
            fg_name_to_items=data['fg_name_to_items'],
            highlighted_items=data['highlighted_items']
        )
    else:
        return render_template('index.html', fg_names=fg_names)

if __name__ == '__main__':
    app.run(debug=True, port=588)