import PyPDF2
import os

merger = PyPDF2.PdfMerger()
input_dir = r"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\PDFs merge\DF-LX-0000CF-M-00012-M-00015_REV_0_C001"
output_dir = r"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\PDFs merge\DF-LX-0000CF-M-00012-M-00015_REV_0_C001"
environ_os = os.environ
# print(environ_os)
lista_arquivos = os.listdir(input_dir)
for arquivo in lista_arquivos:
    if ".pdf" in arquivo:
        print(arquivo)
        merger.append(fr"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\PDFs merge\DF-LX-0000CF-M-00012-M-00015_REV_0_C001/{arquivo}")

merger.write(output_dir+"\DF-LX-0000CF-M-00012-M-00015_REV_0_C001.pdf")




