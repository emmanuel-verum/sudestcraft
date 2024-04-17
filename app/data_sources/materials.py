import pandas as pd
import os
import numpy as np
import json

class Reports():
    project_prefix = [
        'CF-S1985',
        'VG-P0400',
    ]
    def __init__(self, source_dir) -> None:
        for item in os.listdir(source_dir):
            if os.path.isfile(os.path.join(source_dir, item)):
                file_path = os.path.join(source_dir, item)
                if 'recebimento.csv' in item.lower():
                    self.df_recebimento = pd.read_csv(
                        file_path,
                        encoding = 'ANSI',
                        sep=';', 
                        index_col=False,
                        header=0,
                        na_values=['  #VALUE! ', '  #DIV/0! '],
                        usecols=[' TAG/CÓDIGO ', ' QT RECEBIDA ', ' FORNECEDOR ', ' ATTR_VALUE', ' DESCRIÇÃO ']
                    )
                if 'desenho.csv' in item.lower():
                    self.df_desenho = pd.read_csv(
                        file_path,
                        encoding = 'ANSI',
                        sep=';', 
                        index_col=False,
                        header=0,
                        na_values=['  #VALUE! ', '  #DIV/0! '],
                        usecols=[' Nº LM', ' Tag/Código', ' Quantidade em BOM', ' Descrição', ' Peso Unit'],
                        dtype={' Quantidade em BOM':float, ' Peso Unit':float}
                    )
                if 'distribuicao.csv' in item.lower():
                    self.df_distribuicao = pd.read_csv(
                        file_path,
                        encoding = 'ANSI',
                        sep=';', 
                        index_col=False,
                        header=0,
                        na_values=[' / '],
                        usecols=[' DOC REF (ITEM) ', ' CONTRATADA ', ' RESERVA/RODADA ', ' TAG ', ' QT SOLICITADA ', ' QT ENTREGUE ', ' COMENTÁRIOS ']
                    )   
        for prop in  ['df_recebimento', 'df_desenho', 'df_distribuicao']:
            missing_reports = []
            try:
                getattr(self, prop) 
            except:
                missing_reports.append(prop)
        if missing_reports:
            raise FileNotFoundError(f'The files where not found to generate the following tables: {missing_reports}')

    def clean_reports(self):
        self._clean_status_desenho()
        self._clean_recebimento()
        self._clean_distribuicao()


    def _clean_status_desenho(self):
        df = self.df_desenho
        df = df.rename(columns={
            ' Tag/Código': 'tag', 
            ' Quantidade em BOM': 'qtd_desenho',
            ' Descrição': 'descricao',
            ' Peso Unit': 'peso_un_desenho'
        })
        df['tag'] = df['tag'].str.replace(' ', '')
        df['tag'] = df['tag'].str.upper()
        df['cwp'] = df[' Nº LM'].str.replace(' ', '').str.split('/').str[-1]
        df['peso_un_desenho'] == df['peso_un_desenho'].fillna(0)
        df = df.drop(columns=[' Nº LM'])
        df = df.drop_duplicates(subset=['cwp', 'tag'])
        df[['tag', 'descricao', 'cwp']] = df[['tag', 'descricao', 'cwp']].applymap(lambda x: str(x))
        df['peso_un_desenho'] = df['peso_un_desenho'].apply(lambda x: float(x))

        ########################################################################################
        df = df.loc[~df['cwp'].str.contains('VG-P0400-022-S-MT-0101.01-CWP-EMALTO', na=False)]
        df = df.loc[~df['cwp'].str.contains('VG-P0400-115-S-MT-0283.01-CWP-EMALTO ', na=False)]
        ########################################################################################
        self.df_desenho = df

    def _clean_recebimento(self):
        df = self.df_recebimento
        df = df.rename(columns={
            ' TAG/CÓDIGO ': 'tag', 
            ' QT RECEBIDA ': 'qtd_recebida', 
            ' FORNECEDOR ': 'fornecedor',
            ' ATTR_VALUE': 'peso_un_recebimento',
            ' DESCRIÇÃO ': 'descricao'
        })
        df['tag'] = df['tag'].str.upper().str.replace(' ', '')
        df['qtd_recebida'] = df['qtd_recebida'].astype(float)
        df['peso_un_recebimento'] = df['peso_un_recebimento'].astype(float)
        df['fornecedor'] = df['fornecedor'].str.split('-').str[0].str.replace(' ', '')
        df_categorical = df[['tag', 'fornecedor', 'peso_un_recebimento', 'descricao']].drop_duplicates(subset='tag' ,keep='first')
        df_numerical = df[['tag', 'qtd_recebida']].groupby('tag', as_index=False).sum()
        df = pd.merge(
            df_numerical,
            df_categorical,
            on='tag',
            how='left'
        )
        self.df_recebimento = df

    def _clean_distribuicao(self):
        def extract_iwp(comment):
            for string_slice in comment.split(' '):
                if any(prefix in string_slice for prefix in self.project_prefix):
                    sub_slices = string_slice.split('CWP')
                    count=0
                    flag_alpha=False
                    for char in sub_slices[-1][1:]:
                        if char.isalpha():
                            if flag_alpha:
                                break
                            else:
                                flag_alpha = True
                        else:
                            if any(char == special_char for special_char in '(|) '):
                                break
                        count+=1
                    sub_slices[-1] = sub_slices[-1][:count + 1]
                    string_slice = 'CWP'.join(sub_slices)
                    return string_slice
            return np.nan

        df = self.df_distribuicao
        df = df.rename(columns={
            ' DOC REF (ITEM) ': 'cwp',
            ' CONTRATADA ': 'contratada', 
            ' RESERVA/RODADA ': 'iwp',
            ' TAG ': 'tag',
            ' QT SOLICITADA ': 'qtd_solicitada', 
            ' QT ENTREGUE ': 'qtd_entregue',
            ' COMENTÁRIOS ': 'comentarios'
        })
        df['iwp'] = df['iwp']\
            .str.split('/').str[0]\
            .str.replace(' ', '')
        df_iwp = df['comentarios'].apply(extract_iwp).dropna()
        df = pd.merge(
            df, 
            df_iwp, 
            left_index=True, 
            right_index=True,
            suffixes=(None, '_iwp_extracted'),
            how='left'
        )
        df.loc[df['iwp'].isna() | df['iwp'], 'iwp'] = df['comentarios_iwp_extracted']
        df['cwp'] = df['cwp'].str.replace(' ', '')
        df['tag'] = df['tag'].str.replace(' ', '')
        df['qtd_entregue'] = df['qtd_entregue'].str.replace(' ', '')
        df.loc[df['qtd_entregue'] == '', 'qtd_entregue'] = 0
        df.loc[df['qtd_solicitada'] == '', 'qtd_solicitada'] = 0
        df['qtd_entregue'] = df['qtd_entregue'].astype(float)
        df['qtd_solicitada'] = df['qtd_solicitada'].astype(float)
        df[['qtd_solicitada', 'qtd_entregue']] = df[['qtd_solicitada', 'qtd_entregue']].fillna(0) 
        df = df.dropna(subset=['tag'])           
        self.df_distribuicao = df

    def merge_with(self, other_report):
        self.df_distribuicao = pd.concat([self.df_distribuicao, other_report.df_distribuicao])
        self.df_recebimento = pd.concat([self.df_recebimento, other_report.df_recebimento])
        self.df_desenho = pd.concat([self.df_desenho, other_report.df_desenho])
        return self