import pandas as pd
import os
from datetime import datetime




class LX():
    def __init__(self, source_dir, grouped_by_building=False) -> None:
        self.source_dir = source_dir
        self.grouped_by_building = grouped_by_building
        self.config = {
            'extensions': ['.xls', '.xlsx'],
            'ignore':['.SUPERADOS', 'old', 'ETL', 'ROMANEIOS'],
            'depth':3
        }

    def get_report(self):
        self._run_pipeline()
        return self.df_lx

    def get_erros(self):
        self._run_pipeline()
        return self.df_errors

    def _run_pipeline(self):
        self._map_files()
        self._read_files()
        self._clean_raw()

    def _map_files(self):
        def filter_dir(dir_path):
            return dir_path[len(self.source_dir):].count(os.sep) >= self.config['depth'] and all(term not in dir_path for term in self.config['ignore'])

        def filter_files(file_name):
            return any(ext in file_name for ext in self.config['extensions'])

        mapped_dirs = [(root, files) for root, _, files in os.walk(self.source_dir) if filter_dir(root)]
        self.mapped_files = [os.path.join(dir_path, file_name) for dir_path, files in mapped_dirs for file_name in files if filter_files(file_name)]


    def _search_header_params(self, worksheet):
        rev = worksheet.iloc[4,13]
        cod_vale = worksheet.iloc[4,9]
        if 'LX' not in str(cod_vale):
            cod_vale = worksheet.iloc[0,9]
            if 'CF' not in str(cod_vale) and 'LX' not in str(cod_vale):
                cod_vale = worksheet.iloc[0,10]
                if 'CF' not in str(cod_vale) and 'LX' not in str(cod_vale):
                    cod_vale = worksheet.iloc[0,12]
        return {'rev': str(rev), 'cod_vale':cod_vale}


    def _read_files(self):
        worksheets = []
        df_errors = pd.DataFrame(columns=['file_path', 'sheet', 'error'])
        for file_path in self.mapped_files:
            try:
                mod_date = os.path.getmtime(file_path)
                try:
                    workbook = pd.ExcelFile(file_path)
                except:
                    workbook = pd.ExcelFile(file_path, engine='pyxlsb')

                header = {'rev': None, 'cod_vale':None}
                for sheet in workbook.sheet_names:
                    if 'rosto' in sheet.lower():
                        worksheet = workbook.parse(
                            sheet,
                            skiprows=2,
                        )
                        header = self._search_header_params(worksheet)
                    if 'rosto' not in sheet.lower():
                        try:
                            worksheet = workbook.parse(
                                sheet,
                                skiprows=2,
                                na_values=[' ', 'D', '-', '1\n1'],
                                usecols=['CWP', 'PESO UNIT(KG)', 'CÓDIGO DO MATERIAL (SKU)', 'QTDE', 'DESCRIÇÃO COMPLETA', 'OBS', 'TAG DA REFERÊNCIA'],
                                converters={
                                    'CWP': str, 
                                    'CÓDIGO DO MATERIAL (SKU)': str, 
                                    'DESCRIÇÃO COMPLETA': str,  
                                    'OBS': str,  
                                    'TAG DA REFERÊNCIA': str,
                                    'PESO UNIT(KG)': str,
                                }
                            )
                            worksheet['supplier'] = file_path[len(self.source_dir):].split(os.sep)[1]
                            worksheet['sheet_name'] = sheet
                            worksheet['file_name'] = os.path.basename(file_path)
                            worksheet['file_path'] = file_path
                            worksheet['last_mod_date'] = datetime.fromtimestamp(mod_date).strftime('%Y-%m-%d %H:%M:%S')
                            worksheet['rev'] = header['rev']
                            worksheet['cod_vale'] = header['cod_vale'] 
                            worksheets.append(worksheet)
                        except:
                            row = pd.DataFrame({'file_path': [file_path], 'sheet': [sheet], 'error': 'processing'})
                            df_errors = pd.concat([df_errors, row], axis=0, ignore_index=True)        
            except Exception as e:
                row = pd.DataFrame({'file_path': [file_path], 'sheet': None, 'error': 'reading'})
                df_errors = pd.concat([df_errors, row], axis=0, ignore_index=True)                
        self.df_raw = pd.concat(worksheets, axis=0, ignore_index=True)
        self.df_errors = df_errors
        

    def _clean_raw(self):
        df = self.df_raw
        print(df[df["PESO UNIT(KG)"].isna()][['file_path', 'supplier']])
        df = df.rename(columns={
            'CWP': 'cwp',
            'TAG DA REFERÊNCIA': 'cod_ativo',
            'CÓDIGO DO MATERIAL (SKU)': 'tag',
            'DESCRIÇÃO COMPLETA': 'descricao',
            'QTDE': 'qtd_lx', 
            'PESO UNIT(KG)': 'peso_un_lx',
            'OBS': 'obs'
        })
        df = df.dropna(subset=['cwp', 'tag', 'qtd_lx'])
        df['cwp'] = df['cwp'].str.replace(' ', '')
        df['tag'] = df['tag'].str.replace(' ', '')
        
        df.loc[~df['obs'].str.contains('NÃO MODELAD', na=False), 'geometry'] = True
        df.loc[df['obs'].str.contains('NÃO MODELAD', na=False), 'geometry'] = False        
        df['cwp_number'] = df['cwp'].str.split('-').str[-1]
        df['chave'] = df['cwp_number'].str.zfill(3) + '-' + df['tag']
        df.loc[df['peso_un_lx'].str.contains(',', na=False, regex=False), 'peso_un_lx'] = df['peso_un_lx'].str.replace('.', '').str.replace(',', '.')
        print(df[["supplier", "cwp"]])
        df['peso_un_lx'] = df['peso_un_lx'].apply(lambda x: 0 if '-' in str(x) else float(x))
        
        pd.set_option('display.max_colwidth', None)
        df['qtd_lx'] = df['qtd_lx'].astype(float)

        keys = ['cwp', 'cod_ativo', 'tag'] if self.grouped_by_building else ['cwp', 'tag']
        df_numeric = df[keys + ['qtd_lx']].groupby(by=keys, as_index=False).sum(numeric_only=True).round(0)
        df_categorical = df.copy().drop(columns=['qtd_lx']).drop_duplicates(subset=keys, keep='first')
        df = pd.merge(df_numeric, df_categorical, how='left', on=keys)
        df = df.sort_values(by=['rev'], ascending=False).drop_duplicates(subset=['cod_vale', 'cwp', 'tag'], keep='first')

        self.df_lx = df