"""Pipeline desenvolvido para o BI de Montagem Eletromecanica."""
import os
import pandas as pd
from pipelines import pipeline_tools
from data_sources.suppliers import CronogramaMasterConstrucap
from data_sources.LX import LX
from data_sources.foundation import Summary
from data_sources.materials import Reports
from data_sources.masterplan import ListaMaster, Masterplan
from datetime import datetime


def newsteel():
    output_dir = os.environ['OUTPUT_MONTAGEM_ELETROMECANICA_NEWSTEEL']
    summary = Summary(os.environ['SUMMARY_PATH_NEWSTEEL'])
    masterplan = CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    lx = LX(os.environ['LX_PATH_NEWSTEEL'])
    reports.clean_reports()
    
    lx._run_pipeline()
    df_lx = lx.df_lx
    df_error = lx.df_errors
    print("Os seguintes arquivos não puderam ser lidos de forma correta:\n")
    print(df_error)
    df_error.to_excel(os.path.join(output_dir, 'erros_found.xlsx'), index=False)
    
    df_summary = summary.get_report()
    df_masterplan = masterplan.get_report()
    df_recebimento = reports.df_recebimento
    df_desenho = reports.df_desenho
    df_distribuicao = reports.df_distribuicao

    print(df_lx.columns)
    print(df_summary.columns)
    print(df_masterplan.columns)
    print(df_recebimento.columns)
    print(df_desenho.columns)
    print(df_distribuicao.columns)

    df_iwp = pd.merge(
        left=df_summary,
        right=df_desenho,
        on=['cwp', 'tag'],
        how='left',
        suffixes=['_summary', '_desenho']
    ).drop(columns='qtd_desenho').rename(columns={'qtd_summary': 'qtd_desenho'})
    df_iwp = pd.merge(
        left=df_iwp,
        right=df_lx,
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_lx')
    )
    df_distribuicao_no_iwp = df_distribuicao.loc[df_distribuicao['iwp'].isna()]
    df_distribuicao_iwp = df_distribuicao.loc[~df_distribuicao['iwp'].isna()]
    df_distribuicao_iwp = df_distribuicao_iwp[['iwp', 'tag', 'qtd_solicitada', 'qtd_entregue']].groupby(['iwp', 'tag'], as_index=False).sum()
    df_iwp = pd.merge(
        left=df_iwp,
        right=df_distribuicao_iwp,
        on=['iwp', 'tag'],
        how='outer',
        suffixes=[None, '_distribuicao']
    )
    df_iwp = df_iwp.sort_values(by=['data_inicio', 'iwp'], ascending=True)
    df_distribuicao_no_iwp = df_distribuicao_no_iwp[['tag', 'qtd_solicitada', 'qtd_entregue']].groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_iwp = pipeline_tools._predict_stock(df_iwp, df_distribuicao_no_iwp)
    df_iwp = pipeline_tools._get_quantities(df_iwp.sort_values(by='data_inicio', ascending=True), df_recebimento.copy())
    df_iwp = pd.merge(
        df_iwp,
        df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_iwp['supplier'] = df_iwp['supplier'].fillna(df_iwp['fornecedor'])
    df_iwp.loc[df_iwp['peso_un_recebimento'].isna(), 'peso_un'] = df_iwp['peso_un_desenho']
    df_iwp.loc[df_iwp['peso_un'].isna(), 'peso_un'] = df_iwp['peso_un_recebimento']
    df_iwp.to_parquet(os.path.join(output_dir, 'iwp_data.parquet'), index=False)

    df_cwp = pd.merge(
        left=df_desenho,
        right=df_lx,
        on=['cwp', 'tag'],
        how='outer',
        suffixes=(None, '_lx')
    )    
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_summary[['cwp', 'data_inicio']].sort_values(by='data_inicio', ascending=True).drop_duplicates(subset=['cwp'], keep='first'),
        on=['cwp'],
        how='left',
        suffixes=(None, '_summary')
    )    
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_masterplan[['cwp', 'descricao', 'data_inicio']],
        on='cwp',
        how='left',
        suffixes=(None, '_masterplan')
    )    
    df_cwp.loc[df_cwp['data_inicio'].isna(), 'data_inicio'] = df_cwp['data_inicio_masterplan']
    df_cwp = df_cwp.sort_values(by='data_inicio', ascending=True, na_position='last')
    df_cwp = pipeline_tools.get_quantities_montagem_eletromecanica(df_cwp, df_distribuicao.groupby(by=['cwp', 'tag'], as_index=False).sum(numeric_only=True), by=['cwp','tag'])
    df_unknow_cwp = df_distribuicao.loc[~df_distribuicao[['cwp', 'tag']].apply(tuple,1).isin(df_cwp[['cwp', 'tag']].apply(tuple,1)), ['cwp', 'tag', 'qtd_solicitada', 'qtd_entregue']]
    df_unknow_cwp = df_unknow_cwp.groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_cwp = pipeline_tools._predict_stock(df_cwp, df_unknow_cwp)
    df_cwp = pipeline_tools._get_quantities(df_cwp.sort_values(by='data_inicio', ascending=True), df_recebimento)
    df_cwp = pd.merge(
        df_cwp,
        df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_cwp['supplier'] = df_cwp['supplier'].fillna(df_cwp['fornecedor'])
    df_cwp.loc[df_cwp['peso_un_desenho'].isna(), 'peso_un_desenho'] = df_cwp['peso_un_recebimento']
    df_cwp.to_parquet(os.path.join(output_dir, 'cwp_data.parquet'), index=False)
    _get_view(df_cwp, df_iwp, os.path.join(output_dir, 'view_gestao_de_materiais_eletromecanica.xlsx'))



def capanema():
    output_dir = os.environ['OUTPUT_MONTAGEM_ELETROMECANICA_CAPANEMA']
    summary = Summary(os.environ['SUMMARY_PATH_CAPANEMA'])
    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lista_master = ListaMaster(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'])
    lx_sinosteel = LX(r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX\SINOSTEEL\LX_GERAL_SINOSTEEL")
    reports = Reports(source_dir=os.environ['REPORTS_PATH_CAPANEMA'])
    reports.clean_reports()

    df_summary = summary.get_report()
    df_masterplan = masterplan.get_report()
    df_lista_master = lista_master.get_report()
    df_recebimento = reports.df_recebimento
    df_desenho = reports.df_desenho
    df_distribuicao = reports.df_distribuicao

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
    df_lx = pd.concat([df_lx, df_lx_sinosteel])

    df_iwp = pd.merge(
        left=df_summary,
        right=df_desenho,
        on=['cwp', 'tag'],
        how='left',
        suffixes=['_summary', '_desenho']
    ).drop(columns='qtd_desenho').rename(columns={'qtd_summary': 'qtd_desenho'})
    df_iwp = pd.merge(
        left=df_iwp,
        right=df_lx,
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_lx')
    )
    df_distribuicao_no_iwp = df_distribuicao.loc[df_distribuicao['iwp'].isna()]
    df_distribuicao_iwp = df_distribuicao.loc[~df_distribuicao['iwp'].isna()]
    df_distribuicao_iwp = df_distribuicao_iwp[['iwp', 'tag', 'qtd_solicitada', 'qtd_entregue']].groupby(['iwp', 'tag'], as_index=False).sum()
    
    df_iwp = pd.merge(
        left=df_iwp,
        right=df_distribuicao_iwp,
        on=['iwp', 'tag'],
        how='outer',
        suffixes=[None, '_distribuicao']
    )
    df_iwp = df_iwp.sort_values(by=['data_inicio', 'iwp'], ascending=True)
    df_distribuicao_no_iwp = df_distribuicao_no_iwp[['tag', 'qtd_solicitada', 'qtd_entregue']].groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_iwp = pipeline_tools._predict_stock(df_iwp, df_distribuicao_no_iwp)
    df_iwp = pipeline_tools._get_quantities(df_iwp.sort_values(by='data_inicio', ascending=True), df_recebimento.copy())
    df_iwp = pd.merge(
        df_iwp,
        df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_iwp['supplier'] = df_iwp['supplier'].fillna(df_iwp['fornecedor'])
    df_iwp.to_parquet(os.path.join(output_dir, 'iwp_data.parquet'), index=False)
    df_cwp = pd.merge(
        left=df_desenho,
        right=df_lx,
        on=['cwp', 'tag'],
        how='outer',
        suffixes=(None, '_lx')
    )
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_summary[['cwp', 'data_inicio']].sort_values(by='data_inicio', ascending=True).drop_duplicates(subset=['cwp'], keep='first'),
        on=['cwp'],
        how='left',
        suffixes=(None, '_summary')
    )    
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_masterplan[['cwp', 'data_inicio']],
        on='cwp',
        how='left',
        suffixes=(None, '_masterplan')
    )    
    df_cwp = pd.merge(
        left=df_cwp,
        right=df_lista_master[['cwp', 'descricao_cwp']],
        on='cwp',
        how='left',
        suffixes=(None, '_lista_master')
    )
    df_cwp.loc[df_cwp['data_inicio'].isna(), 'data_inicio'] = df_cwp['data_inicio_masterplan']
    df_cwp = df_cwp.sort_values(by='data_inicio', ascending=True, na_position='last')
    df_cwp = pipeline_tools.get_quantities_montagem_eletromecanica(df_cwp, df_distribuicao.groupby(by=['cwp', 'tag'], as_index=False).sum(numeric_only=True), by=['cwp','tag'])
    df_unknow_cwp = df_distribuicao.loc[~df_distribuicao[['cwp', 'tag']].apply(tuple,1).isin(df_cwp[['cwp', 'tag']].apply(tuple,1)), ['cwp', 'tag', 'qtd_solicitada', 'qtd_entregue']]
    df_unknow_cwp = df_unknow_cwp.groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_cwp = pipeline_tools._predict_stock(df_cwp, df_unknow_cwp)
    df_cwp = pipeline_tools._get_quantities(df_cwp.sort_values(by='data_inicio', ascending=True), df_recebimento)
    df_cwp = pd.merge(
        df_cwp,
        df_recebimento[['tag', 'peso_un_recebimento', 'fornecedor']],
        on='tag',
        how='left',
        suffixes=(None, '_recebimento')
    )
    df_cwp['supplier'] = df_cwp['supplier'].fillna(df_cwp['fornecedor'])
    df_cwp.to_parquet(os.path.join(output_dir, 'cwp_data.parquet'), index=False)
    print(df_cwp.columns) 
    print(df_iwp.columns) 
    _get_view(df_cwp, df_iwp, os.path.join(output_dir, 'view_gestao_de_materiais_eletromecanica.xlsx'))
    


def _get_view(df_cwp, df_iwp, output_path):
    now = datetime.now()
    df_cwp['peso_un_agregado'] = df_cwp['peso_un_lx'].fillna(df_cwp['peso_un_desenho']).fillna(df_cwp['peso_un_recebimento'])
    df_cwp['peso_total_materials'] = df_cwp['qtd_desenho'] * df_cwp['peso_un_agregado']
    df_cwp['peso_un_agregado'] = df_cwp['peso_un_agregado']/1000
    df_cwp['qtd_total_solicitada'] = df_cwp['qtd_solicitada'] + df_cwp['qtd_solicitada_alocada']
    df_cwp['qtd_total_entregue'] = df_cwp['qtd_entregue'] + df_cwp['qtd_entregue_alocada']
    df_cwp['peso_solicitado'] = df_cwp['qtd_solicitada'] * df_cwp['peso_un_agregado']
    df_cwp['peso_solicitado_alocado'] = df_cwp['qtd_solicitada_alocada'] * df_cwp['peso_un_agregado']
    df_cwp['peso_total_solicitado'] = df_cwp['peso_solicitado'] + df_cwp['peso_solicitado_alocado']
    df_cwp['peso_entregue'] = df_cwp['qtd_entregue'] * df_cwp['peso_un_agregado']
    df_cwp['peso_entregue_alocado'] = df_cwp['qtd_entregue_alocada'] * df_cwp['peso_un_agregado']
    df_cwp['peso_total_entregue'] = df_cwp['peso_entregue'] + df_cwp['peso_entregue_alocado']

    df_cwp.insert(13, 'prontidao', df_cwp['data_inicio_masterplan'] - pd.Timedelta(days=30))
    df_cwp.insert(3, 'peso_lx', df_cwp['qtd_lx'] * df_cwp['peso_un_agregado'] )
    df_cwp.insert(4, 'peso_desenho', df_cwp['qtd_desenho'] * df_cwp['peso_un_desenho'] /1000)
    df_cwp.insert(5, 'peso_recebido', df_cwp['qtd_recebida'] * df_cwp['peso_un_agregado'] )


    df_iwp['peso_un_agregado'] = df_iwp['peso_un_lx'].fillna(df_iwp['peso_un_desenho']).fillna(df_iwp['peso_un_recebimento'])
    df_iwp['peso_total_materials'] = df_iwp['qtd_desenho'] * df_iwp['peso_un_agregado']
    df_iwp['peso_un_agregado'] = df_iwp['peso_un_agregado']/1000
    df_iwp['qtd_total_solicitada'] = df_iwp['qtd_solicitada'] + df_iwp['qtd_solicitada_alocada']
    df_iwp['qtd_total_entregue'] = df_iwp['qtd_entregue'] + df_iwp['qtd_entregue_alocada']
    df_iwp['peso_solicitado'] = df_iwp['qtd_solicitada'] * df_iwp['peso_un_agregado']
    df_iwp['peso_solicitado_alocado'] = df_iwp['qtd_solicitada_alocada'] * df_iwp['peso_un_agregado']
    df_iwp['peso_total_solicitado'] = df_iwp['peso_solicitado'] + df_iwp['peso_solicitado_alocado']
    df_iwp['peso_entregue'] = df_iwp['qtd_entregue'] * df_iwp['peso_un_agregado']
    df_iwp['peso_entregue_alocado'] = df_iwp['qtd_entregue_alocada'] * df_iwp['peso_un_agregado']
    df_iwp['peso_total_entregue'] = df_iwp['peso_entregue'] + df_iwp['peso_entregue_alocado']

    df_iwp.insert(13, 'prontidao', df_iwp['data_inicio'] - pd.Timedelta(days=30))
    df_iwp.insert(3, 'peso_lx', df_iwp['qtd_lx'] * df_iwp['peso_un_agregado'] )
    df_iwp.insert(4, 'peso_desenho', df_iwp['qtd_desenho'] * df_iwp['peso_un_agregado'] )
    df_iwp.insert(5, 'peso_recebido', df_iwp['qtd_recebida'] * df_iwp['peso_un_agregado'] )
    
    df_view_cwp = df_cwp[[
        'cwp',
        'supplier',
        'peso_un_lx',
        'peso_desenho',
        'peso_recebido',
        'peso_un_agregado',
        'qtd_lx',
        'peso_lx',
        'cod_ativo',
        'qtd_total_solicitada',
        'peso_total_solicitado',
        'qtd_total_entregue',
        'peso_total_entregue',
        'peso_total_materials',
        'peso_recebido',
        'data_inicio',
        'prontidao',
        'data_inicio_masterplan',
    ]]

    df_view_iwp = df_iwp[[
        'cwp',
        'supplier',
        'peso_un_lx',
        'peso_desenho',
        'peso_recebido',
        'peso_un_agregado',
        'qtd_lx',
        'peso_lx',
        'cod_ativo',
        'qtd_total_solicitada',
        'peso_total_solicitado',
        'qtd_total_entregue',
        'peso_total_entregue',
        'peso_total_materials',
        'peso_recebido',
        'data_inicio',
        'prontidao',

        # 'iwp',
        # 'tag',
        # 'qtd_desenho',
        # 'descricao_summary',
        # 'disciplina',
        # 'proposito',
        # 'data_inicio',
        # 'data_termino',
        # 'guid',
        # 'cwp',
        # 'cwa',
        # 'descricao_desenho',
        # 'peso_un_desenho',
        # 'qtd_lx',
        # 'cod_ativo',
        # 'descricao',
        # 'peso_un_lx',
        # 'obs',
        # 'supplier',
        # 'sheet_name',
        # 'file_name',
        # 'file_path',
        # 'last_mod_date',
        # 'rev',
        # 'cod_vale',
        # 'geometry',
        # 'cwp_number',
        # 'chave',
        # 'qtd_solicitada',
        # 'qtd_entregue',
        # 'qtd_solicitada_alocada',
        # 'qtd_entregue_alocada',
        # 'qtd_recebida',
        # 'peso_un_recebimento',
        # 'fornecedor'
    ]]

    df_view_cwp = pd.merge(
        df_view_cwp.groupby(['cwp', 'supplier'], as_index=False).sum(),
        df_view_cwp[[
            'cwp', 
            'supplier', 
            'data_inicio',
            'prontidao', 
            'data_inicio_masterplan',
        ]].drop_duplicates(subset=['cwp', 'supplier']),
        on=['cwp', 'supplier']
    )

    df_view_iwp = pd.merge(
        df_view_iwp.groupby(['cwp', 'supplier'], as_index=False).sum(),
        df_view_iwp[[
            'cwp', 
            'supplier', 
            'data_inicio',
            'prontidao', 
            'data_inicio',
        ]].drop_duplicates(subset=['cwp', 'supplier']),
        on=['cwp', 'supplier']
    )

    df_view_cwp = df_view_cwp.rename(columns={
        'cwp': 'CWP',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'cod_ativo': 'Tag referencia',
        'peso_desenho': 'Peso Materials (t)',
        'peso_recebido': 'Recebido (t)',
        'peso_capex_ton_memoria_fornecedor': 'Peso PQ (t)',
        'data_inicio': 'Start Tendência',
        'prontidao': 'Prontidão',
        'data_inicio_masterplan': 'Inicio de Montagem',
    })

    df_view_iwp = df_view_iwp.rename(columns={
        'cwp': 'CWP',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'cod_ativo': 'Tag referencia',
        'peso_desenho': 'Peso Materials (t)',
        'peso_recebido': 'Recebido (t)',
        'data_inicio': 'Start Tendência',
        'prontidao': 'Prontidão',
        'data_inicio': 'Inicio de Montagem',
    })


    df_view_cwp_tag = df_cwp[[
        'cwp',
        'cod_ativo',
        'tag',
        'supplier',
        'peso_lx',
        'peso_desenho',
        'peso_recebido',
        'qtd_desenho',
        'qtd_recebida',
        'descricao',
    ]].dropna(subset=['tag'])
    
    df_view_iwp_tag = df_cwp[[
        'cwp',
        'cod_ativo',
        'tag',
        'supplier',
        'peso_lx',
        'peso_desenho',
        'peso_recebido',
        'qtd_desenho',
        'qtd_recebida',
        'descricao',
    ]].dropna(subset=['tag'])

    df_view_cwp_tag = df_view_cwp_tag.rename(columns={
        'cwp': 'CWP',
        'cod_ativo': 'Tag referencia',
        'tag': 'TAG',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'peso_desenho': 'Peso Materials (t)',
        'qtd_desenho': 'Qtd. Materials',
        'peso_recebido': 'Recebido (t)',
        'qtd_recebida': 'Qtd. Recebimento',
        'descricao_desenho': 'Descrição Materials',
    })

    df_view_iwp_tag = df_view_iwp_tag.rename(columns={
        'cwp': 'CWP',
        'cod_ativo': 'Tag referencia',
        'tag': 'TAG',
        'supplier': 'Fornecedor',
        'peso_lx': 'Peso LX (t)',
        'peso_desenho': 'Peso Materials (t)',
        'qtd_desenho': 'Qtd. Materials',
        'peso_recebido': 'Recebido (t)',
        'qtd_recebida': 'Qtd. Recebimento',
        'descricao_desenho': 'Descrição Materials',
    })

    writer = pd.ExcelWriter(output_path, engine = 'xlsxwriter')
    df_view_cwp_tag.to_excel(writer, sheet_name = 'Dados de cwp por Tag', index=False, startrow=1)
    df_view_cwp.to_excel(writer, sheet_name = 'Dados de cwp por CWP', index=False)
    df_view_iwp_tag.to_excel(writer, sheet_name = 'Dados de iwp por Tag', index=False)
    df_view_iwp.to_excel(writer, sheet_name = 'Dados de cwp por iWP', index=False)
    worksheet = writer.sheets['Dados de cwp por Tag']
    worksheet.write(0, 0, f'Atualizado em {str(now.date())} {str(now.time().hour)}:{str(now.time().minute)}')
    writer.close()
