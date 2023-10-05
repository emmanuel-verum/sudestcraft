"""Pipeline desenvolvido para automação da atualização do modelo federado. Aqui, os status de produção/entrega são
inseridos de volta nos modelos IFC, que serão posteriormente federados em um único arquivo NWD com ajuda do Navis"""
import pandas as pd
import os
from pipelines import pipeline_tools
from data_sources.materials import Reports
from data_sources.ifc_sources import TracerFullReport, Vcad
from data_sources.masterplan import Masterplan
from data_sources.suppliers import CronogramaMasterConstrucap
from data_sources.LX import LX
import ifcopenshell
import ifcopenshell.api

pd.options.mode.chained_assignment = None


def newsteel():
    output_dir = os.environ['FEDERATED_PATH_NEWSTEEL']
    ifc_dir = os.environ['IFC_PATH_NEWSTEEL']

    cronograma_construcap = CronogramaMasterConstrucap(os.environ['MONTADORA_PATH_NEWSTEEL'])
    lx = LX(os.environ['LX_PATH_NEWSTEEL'])
    reports = Reports(os.environ['REPORTS_PATH_NEWSTEEL'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_NEWSTEEL'])
    df_lx = lx.get_report()
    reports.clean_reports()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])

    df_main = pd.merge(
        left=df_lx,
        right=reports.df_desenho, 
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_materials')
    )

    df_main = pd.merge(
        left=df_main,
        right=cronograma_construcap.get_report(), 
        on='cwp',
        how='left'
    )
    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.df_recebimento)
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']  
    
    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report()
    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))]
    df_main = df_main.loc[df_main['cwp'].isin(df_tracer['cwp'].drop_duplicates(keep='first'))]

    print(df_tracer.columns)
    print(df_main.columns)

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un_lx']],
        on=['cwp', 'tag'],
        how='left'
    )  

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_codeme, axis=1)
    for item in os.listdir(ifc_dir):
        print('Processing file: ', item)
        try:
            ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
            file_name = item.replace('.ifc', '')         
            for idx, row in  df_tracer.loc[df_tracer['file_name'] == file_name].iterrows():
                row = row.fillna(0)
                row['data_inicio'] = '' if row['data_inicio'] == 0 else row['data_inicio']
                row['status'] = '' if row['status'] == 0 else row['status']
                try:
                    element = ifc.by_guid(row['agg_id'])
                    pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
                    ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                        'Status de Entrega': str(row['status']),
                        'Início de Montagem': str(row['data_inicio']).split(' ')[0]
                    })
                except:
                    print(f'Error processing Id {row["agg_id"]}')
        except:
            print(f'Unable to read file{item}')
        ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  


def capanema():
    output_dir = os.environ['FEDERATED_PATH_CAPANEMA']
    ifc_dir = os.environ['IFC_PATH_CAPANEMA']

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    reports = Reports(os.environ['REPORTS_PATH_CAPANEMA'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_CAPANEMA'])
    lx_sinosteel = LX(r'C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX\SINOSTEEL\LX_GERAL_SINOSTEEL')
    lx_sinosteel.config['depth'] = 0
    lx_sinosteel._run_pipeline()
    df_lx_sinosteel = lx_sinosteel.df_lx  
    df_lx_sinosteel['supplier'] = 'SINOSTEEL'
    lx._run_pipeline()
    df_lx = lx.df_lx
    df_lx = df_lx.loc[df_lx['supplier'] != 'SINOSTEEL']

    df_lx = pd.concat([df_lx, df_lx_sinosteel])
    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])

    reports.clean_reports()
    df_main = pd.merge(
        left=df_lx,
        right=masterplan.get_report(), 
        on='cwp',
        how='left'
    )
    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.df_recebimento)
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']  
    df_main["qtd_desenho"] = df_main["qtd_lx"]
    df_distribuicao = reports.df_distribuicao
    df_main = pipeline_tools.get_quantities_montagem_eletromecanica(df_main, df_distribuicao.groupby(by=['cwp', 'tag'], as_index=False).sum(numeric_only=True), by=['cwp','tag'])
    df_unknow_cwp = df_distribuicao.loc[~df_distribuicao[['cwp', 'tag']].apply(tuple,1).isin(df_main[['cwp', 'tag']].apply(tuple,1)), ['cwp', 'tag', 'qtd_solicitada', 'qtd_entregue']]
    df_unknow_cwp = df_unknow_cwp.groupby(by=['tag'], as_index=False).sum(numeric_only=True)
    df_main = pipeline_tools._predict_stock(df_main, df_unknow_cwp)
    df_main["qtd_solicitada_total"] = df_main["qtd_solicitada"]+df_main["qtd_solicitada_alocada"]
    df_main["qtd_entregue_total"] = df_main["qtd_entregue"]+df_main["qtd_entregue_alocada"]

    # descomente a linha abaixo e comente o bloco acima para não precisar processar df_main novamente
    # df_main = pd.read_parquet(r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\Stagging\main.parquet")
    df_cronograma_SK = pd.read_excel(r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\4D\Status_Federado\CF-S1985-008\Cronograma SK por IWP - Circuito Terciário_2023-08-13.xlsx")

    df_cronograma_SK = df_cronograma_SK.rename(columns={
        'SK - IWP': 'iwp',
        'Physical % Complete': 'Avanco',
    })


    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report() #carregando as tabelas da staging

    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))] # filtrando comuns entre df_tracer e df_main por 'cwp'
    df_main = df_main.loc[df_main['cwp'].isin(df_tracer['cwp'].drop_duplicates(keep='first'))] # filtrando comuns entre df_main e df_tracer  por 'cwp'
    
    # descomente a linha abaixo para gerar uma stagging de df_main e não precisar processar novamente
    # df_main.to_parquet(r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\Stagging\main.parquet", index=False)

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_solicitada_total', 'qtd_entregue_total']],
        on=['cwp', 'tag'],
        how='left',
        
    )

    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_distribuicao[['cwp', 'tag', 'comentarios_iwp_extracted', 'contratada']],
        on=['cwp', 'tag'],
        how='left'
    )

    # df_tracer = pd.merge(
    #         left=df_tracer, 
    #         right=df_cronograma_SK[[ 'iwp', 'Avanco']],
    #         on=['iwp'],
    #         how='left'
    #     )

    #print(df_consolidada[df_consolidada['comentarios_iwp_extracted'].notna()])
    # print("cronograma")
    # print(df_cronograma_SK[df_cronograma_SK[ 'Avanco'].notna()])
    print("distribuição")
    print(df_distribuicao[df_distribuicao[ 'iwp'].notna()])
    print("main")
    print(df_main.columns)
    print(df_main[['cwp', 'tag', 'cod_ativo', 'descricao', 'obs', 'chave',]])
    print("tracer")
    print(df_tracer.columns)

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_distribuicao_sinosteel, axis=1)
    
    # df_view_tracer = df_tracer.loc[ (df_tracer['status'] == '6.Marca inconsistente'), ['cwp', 'agg_id', 'order']] #listando os itens com divergencia na tabela tracer
    # df_view_tracer.to_excel(r"C:\Users\RafaelSouza\OneDrive\Documentos\df_tracer.xlsx", index=False) 

    # stagging df_tracer
    #df_tracer.to_parquet(r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\Stagging\tracer.parquet", index=False)
    
    # lendo df_tracer do stagging
    # df_tracer = pd.read_parquet(r"C:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\BI\02. Repositório de Arquivos\Modelos BIM\Stagging\tracer.parquet")

    print(df_tracer[['status', 'supplier']].drop_duplicates())
    print(df_tracer['status'].value_counts())
    
    

    for item in os.listdir(ifc_dir):
        # if  'CF-S1985-008-M-MT-CWP-830-TR-1220CF-11' in item :
            if any(cwp_file in item for cwp_file in item):
                print('Processing file: ', item)
                try:
                    ifc = ifcopenshell.open(os.path.join(ifc_dir, item))
                    file_name = item.replace('.ifc', '') 

                    for idx, row in  df_tracer.loc[df_tracer['file_name'] == file_name].iterrows():
                        row = row.fillna(0)
                        row['status'] = '' if row['status'] == 0 else row['status']
                        row['comentarios_iwp_extracted'] = '' if row['comentarios_iwp_extracted'] == 0 else row['comentarios_iwp_extracted']
                        try:
                            element = ifc.by_guid(row['agg_id'])
                            pset = ifcopenshell.api.run("pset.add_pset", ifc, product=element, name="Verum")
                            ifcopenshell.api.run("pset.edit_pset", ifc, pset=pset, properties={
                                'Status de Entrega': str(row['status']),
                                'iwp': str(row['comentarios_iwp_extracted'])
                                                    })
                        except:
                            print(f'Error processing Id {row["agg_id"]} in file: {file_name}')
                except:
                    print(f'Unable to read file: {item}')
                ifc.write(os.path.join(output_dir, file_name + '_edited.ifc'))  


def vcad():
    output_dir = os.environ['OUTPUT_FEDERADO_CAPANEMA'] 

    masterplan = Masterplan(os.environ['MASTERPLAN_PATH_CAPANEMA'])
    lx = LX(os.environ['LX_PATH_CAPANEMA'], os.environ['MAPPER_PATH_CAPANEMA'])
    reports = Reports(os.environ['REPORTS_PATH_CAPANEMA'])
    tracer = TracerFullReport(os.environ['TRACER_PATH_CAPANEMA'])
    vcad = Vcad(os.environ['VCAD_PATH_CAPANEMA'])
    
    df_lx = lx.get_report()

    df_numeric = df_lx[['cwp', 'tag', 'qtd_lx']].groupby(['cwp', 'tag'], as_index=False).sum(numeric_only=True)
    df_categorical = df_lx.drop(columns=['qtd_lx']).drop_duplicates(subset=['cwp', 'tag'], keep='first')
    df_lx = pd.merge(df_numeric, df_categorical, how='left', on=['cwp', 'tag'])
    df_lx = df_lx.loc[df_lx['supplier'].str.contains('CODEME', na=False) | df_lx['supplier'].str.contains('SINOSTEEL', na=False)]

    df_main = pd.merge(
        left=df_lx,
        right=reports.get_status_desenho(), 
        on=['cwp', 'tag'],
        how='left',
        suffixes=(None, '_materials')
    )
    
    df_main = pd.merge(
        left=df_main,
        right=masterplan.get_report(), 
        on='cwp',
        how='left'
    )
    
    df_main = pipeline_tools.get_quantities(df_main.sort_values(by='data_inicio', ascending=True), reports.get_recebimento())
    df_main['qtd_faltante'] = df_main['qtd_lx'] - df_main['qtd_recebida']
    df_main['cod_navegacao'] = df_main['cwp_number'] + '-' + df_main['cod_ativo']
    df_main['chave'] = df_main['cwp'] + '-' + df_main['tag']

    df_fill = df_main[['cwp', 'cod_navegacao']].drop_duplicates(subset=['cwp'], keep='first')
    df_fill['chave'] = df_fill['cwp']
    df_main = pd.concat([df_main,df_fill], ignore_index=True)

    df_tracer = tracer.read_stagging_data().drop_missplaced_elements().get_report()
    df_tracer = df_tracer.loc[df_tracer['cwp'].isin(df_main['cwp'].drop_duplicates(keep='first'))]
    df_main = df_main.loc[df_main['cwp'].isin(df_tracer['cwp'].drop_duplicates(keep='first'))]

    df_tracer['cod_ativo'] = df_tracer['file_name'].str[26:]
    df_tracer = df_tracer.loc[df_tracer['cwp'] == df_tracer['file_name'].str[0:25]]
    df_tracer = pd.merge(
        left=df_tracer, 
        right=df_main[['cwp', 'tag', 'qtd_recebida', 'qtd_lx', 'qtd_desenho', 'qtd_faltante', 'data_inicio', 'peso_un']],
        on=['cwp', 'tag'],
        how='left'
    )  

    df_tracer['status'] = df_tracer.apply(pipeline_tools.apply_status_codeme, axis=1)
    df_tracer.loc[~df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), ['chave', ]] = df_tracer['cwp']
    df_tracer.loc[df_tracer['status'].isin(['1.Recebido', '2.Não entregue']), 'chave'] = df_tracer['cwp'] + "-" +df_tracer['tag']
    
    df_vcad = vcad.get_report()
    df = pd.merge(
        df_vcad,
        df_tracer[['IfcId', 'file_name', 'supplier', 'name', 'tag', 'cwp', 'cod_ativo', 'qtd_recebida', 'status', 'agg_id']],
        on='IfcId',
        how='left'
    )

    df.to_parquet(os.path.join(output_dir, 'vcad.parquet'), index=False)