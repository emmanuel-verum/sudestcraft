from config.config import run_config
from dotenv import load_dotenv


load_dotenv('.env')
run_config()

from pipelines import (
    bim_x_pwp, 
    extract_ifc_data, 
    federated_model, 
    gestao_materiais, 
    montagem_eletromecanica, 
    pipeline_tools,
    fix_ifc
)

if __name__ == "__main__":
    # todos extracts são utilizados quando tem algum modelo ifc novo, pega o modelo 3d e transforma em tabela para pasta staging
    # Em caso de erro ou atualização de arquivo separado precisa rodar o extraxt novamente
    # arquivos problematicos:
    #'CF-S1985-008-M-MT-CWP-094-TR-1220CF-03.db'
    # extract_ifc_data.sinosteel(use_files=[ 
    # 'CF-S1985-009-M-MT-CWP-114-TR-1380CF-01.db',
    # 'CF-S1985-014-S-MT-CWP-169-ED-1690CF-01.db',
    # 'CF-S1985-014-S-MT-CWP-169-ED-1690CF-02.db',
    # 'CF-S1985-014-S-MT-CWP-169-ED-1690CF-04.db',
    # 'CF-S1985-014-S-MT-CWP-169-ED-1690CF-05.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1610CF-01.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1610CF-02.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1610CF-03.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1610CF-08.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1610CF-09.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1650CF-02.db',
    # 'CF-S1985-015-S-MT-CWP-180-ED-1650CF-04.db',
    # 'CF-S1985-033-S-MT-CWP-873-ED-1730CF-01.db'
    # ])
    # extract_ifc_data.sinosteel(use_files='CF-S1985-008-M-MT-CWP-094-TR-1220CF-03.db')
    # extract_ifc_data.sinosteel()
    # extract_ifc_data.codeme()
    # extract_ifc_data.emalto('DF-ML-8000VG-S-500003-S-500032_REV_2-CWP_EMALTO.db')
    # extract_ifc_data.famsteel()
    # lx_check.newsteel()
    # 
    #  montagem_eletromecanica.newsteel() #Bi de mesmo nome
    #  gestao_materiais.newsteel() #Bi de mesmo nome conhecido por Fornecimento
    #  bim_x_pwp.emalto() #BI dd Fornecimento
    #  bim_x_pwp.famsteel() #BI dd Fornecimento
    #  federated_model.newsteel()

     montagem_eletromecanica.capanema() #Bi de mesmo nome
     gestao_materiais.capanema() #BI de mesmo nome conhecido por Fornecimento
    #  bim_x_pwp.codeme() #BI dd Fornecimento
    #  bim_x_pwp.sinosteel() #BI dd Fornecimento
    # todos os federados são os modelos ifc com status de entrega atrelados
    # federated_model.capanema()
    # federated_model.vcad()

    # fix_ifc.capanema("type")