"""Pipeline desenvolvido para o BI de Gestão de Materiais"""

import pandas as pd
import data_sources.foundation
import data_sources.suppliers
import os
import datetime
from pipelines import pipeline_tools
from data_sources import suppliers
from data_sources.LX import LX
from data_sources.materials import Reports
from data_sources.masterplan import Masterplan
from pathlib import Path
from ext.converters import convert_xls_to_xlsx
def newsteel():
    output_dir = os.environ['OUTPUT_GESTAO_MATERIAIS_NEWSTEEL']

    construcap = suppliers.CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    aumond = suppliers.Aumond(os.environ['FORNECEDORES_PATH_NEWSTEEL'])
    fam_mining = suppliers.FamMining(os.environ['FORNECEDORES_PATH_NEWSTEEL'])
    fam_structure = suppliers.FamStructure(os.environ['FORNECEDORES_PATH_NEWSTEEL'])
    pq = suppliers.PQSimplified(os.environ['PQ_PATH_NEWSTEEL'])
    lx = LX(os.environ['LX_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    reports.clean_reports()

    df_construcap = construcap.get_report()
    df_aumond = aumond.get_report()
    df_fam_mining = fam_mining.get_report()
    df_fam_structure = fam_structure.get_report()
    df_pq = pq.get_report()
    lx._run_pipeline()
    df_lx = lx.df_lx
    df_error = lx.df_errors
    print("Os seguintes arquivos não puderam ser lidos de forma correta:\n")
    print(df_error)
    df_error.to_excel(os.path.join(output_dir, 'erros_found.xlsx'), index=False)
    # print(df_lx.loc[df_lx.supplier.str.contains("DELTADUCON")])
    df_suppliers = pd.concat([df_aumond, df_fam_mining, df_fam_structure])
    df_suppliers = df_suppliers.sort_values(by='data_termino', ascending=True).drop_duplicates(subset='cwp' ,keep='last')

    df_main = pd.merge(
        left=df_construcap,
        right=df_suppliers,
        on='cwp',
        how='left',
        suffixes=('_construcap', '_fornecedor')
    )

    print(df_construcap.columns)
    
    df_main = pd.merge(
        left=df_main,
        right=df_pq,
        on='cwp',
        how='left',
        suffixes=('_cronograma', '_pq')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_lx[['cwp', 'tag', 'cod_ativo', 'supplier', 'qtd_lx', 'peso_un_lx']],
        on='cwp',
        how='outer',
        suffixes=(None, '_lx')
    )
    print(df_main.columns)
    # print(df_main.loc[df_main.supplier.str.contains("DELTADUCON", na=False), ["cwp", "tag", "qtd_lx"]])
    df_main = pd.merge(
        left=df_main,
        right=reports.df_desenho,
        on=['cwp', 'tag'],
        how='outer',
        suffixes=('_lx', '_desenho')
    )  

    

    print(df_main.columns)

    df_main = pipeline_tools._get_quantities(df_main, reports.df_recebimento)
    df_main = pd.merge(
        df_main,
        reports.df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor', 'descricao']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_main['supplier'] = df_main['supplier'].fillna(df_main['fornecedor'])
    
    print(df_main.columns)
    
    df_main['descricao'] = df_main['descricao_lx'].fillna(df_main['descricao']).fillna(df_main['descricao_desenho'])
    # df_main.loc[df_main['peso_un_recebimento'].isna(), 'peso_un'] = df_main['peso_un_desenho']
    # df_main.loc[df_main['peso_un'].isna(), 'peso_un'] = df_main['peso_un_recebimento'] 

    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)
    _get_view_newsteel(df_main, os.path.join(output_dir, 'view_gestao_de_materiais.xlsx'))    



def capanema():
    output_dir=os.environ['OUTPUT_GESTAO_MATERIAIS_CAPANEMA']

    memoria_calculo = suppliers.MemoriaCalculo(os.environ['MEMORIA_CALCULO_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'])
    lx_sinosteel = LX(r'C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX\SINOSTEEL\LX_GERAL_SINOSTEEL')
    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    reports = Reports(source_dir=os.environ['REPORTS_PATH_CAPANEMA'])
    reports.clean_reports()

    flsmidth = suppliers.ModeloCronogramaCapanema('flsmidth', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    sinosteel_p1 = suppliers.ModeloCronogramaCapanema('sinosteel pct. 1', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    sinosteel_p2 = suppliers.ModeloCronogramaCapanema('sinosteel pct. 2', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    alfa = suppliers.ModeloCronogramaCapanema('alfa', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    codeme = suppliers.ModeloCronogramaCapanema('codeme', os.environ['FORNECEDORES_PATH_CAPANEMA'])
    dpi = suppliers.ModeloCronogramaCapanema('dpi', os.environ['FORNECEDORES_PATH_CAPANEMA'])

    df_suppliers = pd.concat([
        flsmidth.get_report(), 
        sinosteel_p1.get_report(),
        sinosteel_p2.get_report(),
        alfa.get_report(),
        codeme.get_report(),
        dpi.get_report()
    ])
    df_suppliers = df_suppliers.sort_values(by='data_termino', ascending=True).drop_duplicates(subset='cwp' ,keep='last')
    df_suppliers['data_inicio'] = df_suppliers['data_inicio'].dt.date
    df_suppliers['data_termino'] = df_suppliers['data_termino'].dt.date

    df_masterplan = masterplan.get_report()
    lx_sinosteel.config['depth'] = 0
    lx_sinosteel._run_pipeline()
    df_lx_sinosteel = lx_sinosteel.df_lx   
    df_lx_sinosteel['supplier'] = 'SINOSTEEL'
    
    lx._run_pipeline()
    df_lx = lx.df_lx
    df_error = lx.df_errors
    print("Os seguintes arquivos não puderam ser lidos de forma correta:\n")
    print(df_error)
    df_error.to_excel(os.path.join(output_dir, 'erros_found.xlsx'), index=False)

    df_lx = df_lx.loc[df_lx['supplier'] != 'SINOSTEEL']
    # verificação se o material existe ou não
    df_lx = pd.concat([df_lx, df_lx_sinosteel])
    memoria_calculo._clean_report()
    df_memoria = memoria_calculo.report
    df_demontagem = memoria_calculo.cwp_desmontagem

    df_main = pd.merge(
        left=df_masterplan,
        right=df_suppliers,
        on='cwp',
        how='left',
        suffixes=('_masterplan', '_fornecedor')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_memoria[['cwp', 'peso_capex_ton']].groupby('cwp', as_index=False).sum(),
        on='cwp',
        how='left',
        suffixes=(None, '_memoria_cwp')
    )
    df_main = pd.merge(
        left=df_main,
        right=df_lx[['cwp', 'cod_ativo', 'tag', 'supplier', 'qtd_lx', 'peso_un_lx']],
        on='cwp',
        how='left',
        suffixes=(None, '_lx')
    )
    df_main = pd.merge(
        left=df_main,
        right=reports.df_desenho,
        on=['cwp', 'tag'],
        how='outer',
        suffixes=(None, '_desenho')
    )
    df_main = pipeline_tools._get_quantities(df_main.sort_values(by='data_inicio_masterplan', ascending=True), reports.df_recebimento)
    df_main = pd.merge(
        df_main,
        reports.df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_main['supplier'] = df_main['supplier'].fillna(df_main['fornecedor'])

    df_main = pd.merge(
        left=df_main,
        right=df_memoria[['cwp', 'fornecedor', 'peso_capex_ton']].rename(columns={'fornecedor':'supplier'}).groupby(['cwp','supplier'], as_index=False).sum(),
        on=['cwp','supplier'],
        how='outer',
        suffixes=(None, '_memoria_fornecedor')
    )
    df_main = df_main.loc[~df_main['cwp'].isin(df_demontagem['cwp'])]

    df_missing_supplier = df_main.loc[df_main['supplier'].isna() & ~df_main['peso_capex_ton'].isna(), ['cwp']].drop_duplicates(keep='first')
    df_missing_supplier['supplier_flag'] = False
    df_main = pd.merge(
        df_main,
        df_missing_supplier,
        on='cwp',
        how='left',
    )
    #############FILTERS
    # df_main = df_main.loc[df_main['cwp'].str.contains('-S1985-', na=False) | df_main['cwp'].str.contains('CWP NÃO ENCONTRADO', na=False)]
    df_main = df_main.loc[~df_main['cwp'].str.contains('-M-SD-', na=False)]
    df_main = df_main.loc[~df_main['supplier'].isin(["00000004" ,"4100680511","4100690394","5500091398","90000959","90002705"])]
    #############FILTERS

    
    df_main.to_parquet(os.path.join(output_dir, 'report_data.parquet'), index=False)
    df_memoria.to_parquet(os.path.join(output_dir, 'memoria_calculo.parquet'), index=False)
    print(df_main.columns)
    _get_view(df_main, os.path.join(output_dir, 'view_gestao_de_materiais.xlsx'))    


def _get_view(df_view, output_path):
    now = datetime.datetime.now()
    df_view['peso_un_agregado'] = df_view['peso_un_lx'].fillna(df_view['peso_un_desenho']).fillna(df_view['peso_un_recebimento'])
    df_view['peso_un_agregado'] = df_view['peso_un_agregado']/1000
    df_view['peso_total_materials'] = df_view['qtd_desenho'] * df_view['peso_un_agregado']
    df_view.insert(13, 'prontidao', df_view['data_inicio_masterplan'] - pd.Timedelta(days=30))
    df_view.insert(3, 'peso_lx', df_view['qtd_lx'] * df_view['peso_un_agregado'] )
    df_view.insert(4, 'peso_desenho', df_view['qtd_desenho'] * df_view['peso_un_agregado'])
    df_view.insert(5, 'peso_recebido', df_view['qtd_recebida'] * df_view['peso_un_agregado'] )
    


    df_view_cwp = df_view[[
        'cwp',
        'supplier',
        'peso_un_lx',
        'peso_desenho',
        'peso_recebido',
        'peso_un_agregado',
        'peso_capex_ton_memoria_fornecedor',
        'qtd_lx',
        'peso_lx',
        'peso_total_materials',
        'peso_recebido',
        'data_inicio_fornecedor',
        'data_termino_fornecedor',
        'prontidao',
        'data_inicio_masterplan',
        'descricao'
    ]]
    df_view_cwp = pd.merge(
        df_view_cwp.groupby(['cwp', 'supplier'], as_index=False).sum(),
        df_view_cwp[[
            'cwp', 
            'supplier', 
            'data_inicio_fornecedor', 
            'data_termino_fornecedor', 
            'prontidao', 
            'data_inicio_masterplan',
        ]].drop_duplicates(subset=['cwp', 'supplier']),
        on=['cwp', 'supplier']
    )


    df_view_cwp = df_view_cwp.rename(columns={
        'cwp': 'CWP',
        'supplier': 'Fornecedor',
        'peso_un_lx': 'Peso Unitário LX (t)',
        'peso_un_agregado': 'Peso unitário Agregado (t)',
        'peso_recebido': 'Recebido (t)',
        'peso_capex_ton_memoria_fornecedor': 'Peso PQ (t)',
        'qtd_lx': 'Quantidade LX',
        'peso_lx': 'Peso total LX (t)',
        'peso_total_materials': 'Peso total Materials (t)',
        'peso_recebido': 'Peso Total Recebido',
        'data_inicio_fornecedor': 'Start Tendência',
        'data_termino_fornecedor': 'Finish Tendência',
        'prontidao': 'Prontidão',
        'data_inicio_masterplan': 'Inicio de Montagem',
        'peso_un_agregado': 'Peso Unitário Agregado',
    })

    df_view_tag = df_view[[
        'cwp',
        'cod_ativo',
        'tag',
        'supplier',
        'peso_lx',
        'peso_un_agregado',
        'peso_desenho',
        'peso_recebido',
        'qtd_desenho',
        'qtd_recebida',
        'descricao',
    ]].dropna(subset=['tag'])
    
    df_view_tag = df_view_tag.rename(columns={
        'cwp': 'CWP',
        'cod_ativo': 'Cod Ativo',
        'tag': 'TAG',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'peso_un_agregado': 'Peso Unitário Agregado(t)',
        'peso_desenho': 'Peso Materials (t)',
        'qtd_desenho': 'Qtd. Materials',
        'peso_recebido': 'Recebido (t)',
        'qtd_recebida': 'Qtd. Recebimento',
        'descricao': 'Descrição Materials',
    })

    writer = pd.ExcelWriter(output_path, engine = 'xlsxwriter')
    df_view_tag.to_excel(writer, sheet_name = 'Dados por Tag', index=False, startrow=1)
    df_view_cwp.to_excel(writer, sheet_name = 'Dados por CWP', index=False)
    worksheet = writer.sheets['Dados por Tag']
    worksheet.write(0, 0, f'Atualizado em {str(now.date())} {str(now.time().hour)}:{str(now.time().minute)}')
    writer.close()


def _get_view_newsteel(df_view, output_path):
    now = datetime.datetime.now()
    df_view['peso_un_agregado'] = df_view['peso_un_lx'].fillna(df_view['peso_un_desenho']).fillna(df_view['peso_un_recebimento'])
    df_view['peso_un_agregado'] = df_view['peso_un_agregado']/1000
    df_view['peso_total_materials'] = df_view['qtd_desenho'] * df_view['peso_un_agregado']
    df_view.insert(13, 'prontidao', df_view['data_inicio_construcap'] - pd.Timedelta(days=30))
    df_view.insert(3, 'peso_lx', df_view['qtd_lx'] * df_view['peso_un_agregado'])
    df_view.insert(4, 'peso_desenho', df_view['qtd_desenho'] * df_view['peso_un_agregado'])
    df_view.insert(5, 'peso_recebido', df_view['qtd_recebida'] * df_view['peso_un_agregado'])
    


    df_view_cwp = df_view[[
        'cwp',
        'supplier',
        'peso_un_lx',
        'peso_desenho',
        'peso_recebido',
        'peso_un_agregado',
        'peso_total_kg',
        'qtd_lx',
        'peso_lx',
        'cod_ativo',
        'peso_total_materials',
        'peso_recebido',
        'data_inicio_fornecedor',
        'data_termino_fornecedor',
        'prontidao',
        'data_inicio_construcap',
        'descricao'
    ]]
    
    df_view_cwp = pd.merge(
        df_view_cwp.groupby(['cwp', 'supplier'], as_index=False).sum(),
        df_view_cwp[[
            'cwp', 
            'supplier', 
            'data_inicio_fornecedor', 
            'data_termino_fornecedor', 
            'prontidao', 
            'data_inicio_construcap',
            'descricao'
        ]].drop_duplicates(subset=['cwp', 'supplier']),
        on=['cwp', 'supplier']
    )
    df_view_cwp = df_view_cwp.rename(columns={
        'cwp': 'CWP',
        'cod_ativo': 'Cod Ativo',
        'tag': 'TAG',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'cod_ativo': 'Tag referencia',
        'peso_desenho': 'Peso Materials (t)',
        'qtd_desenho': 'Qtd. Materials',
        'peso_recebido': 'Recebido (t)',
        'qtd_recebida': 'Qtd. Recebimento',
        'data_inicio_construcap': 'Inicio de Montagem',
    })

    df_view_tag = df_view[[
        'cwp',
        'tag',
        'supplier',
        'peso_lx',
        'cod_ativo',
        'peso_un_agregado',
        'peso_desenho',
        'peso_recebido',
        'qtd_desenho',
        'qtd_recebida',
        'descricao',
    ]].dropna(subset=['tag'])
    
    df_view_tag = df_view_tag.rename(columns={
        'cwp': 'CWP',
        'tag': 'TAG',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'cod_ativo': 'Tag referencia',
        'peso_un_agregado': 'Peso Unitário Agregado(t)',
        'peso_desenho': 'Peso Materials (t)',
        'qtd_desenho': 'Qtd. Materials',
        'peso_recebido': 'Recebido (t)',
        'qtd_recebida': 'Qtd. Recebimento',
        'descricao': 'Descrição Materials',
    })

    writer = pd.ExcelWriter(output_path, engine = 'xlsxwriter')
    df_view_tag.to_excel(writer, sheet_name = 'Dados por Tag', index=False, startrow=1)
    df_view_cwp.to_excel(writer, sheet_name = 'Dados por CWP', index=False)
    worksheet = writer.sheets['Dados por Tag']
    worksheet.write(0, 0, f'Atualizado em {str(now.date())} {str(now.time().hour)}:{str(now.time().minute)}')
    writer.close()


