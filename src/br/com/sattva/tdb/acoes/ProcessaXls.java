package br.com.sattva.tdb.acoes;

import java.io.File;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.Collection;

import com.sankhya.util.TimeUtils;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class ProcessaXls implements AcaoRotinaJava {

	@Override
	public void doAction(ContextoAcao contextoAcao) throws Exception {
		if (contextoAcao.getLinhas().length > 1) {
			contextoAcao.setMensagemRetorno("Não é permitido processar mais de uma linha");
			return;
		}
		
		String nomeArquivo = (String) contextoAcao.getParam("NOMEARQUIVO");		
		JapeWrapper anexoDAO = JapeFactory.dao("AnexoSistema");
		DynamicVO anexoVO = anexoDAO.findOne("NOMEINSTANCIA = 'AD_CONCILIACKONCILI' AND DESCRICAO = ?", nomeArquivo);
		
		if (anexoVO == null) {
			contextoAcao.mostraErro("Não foi encontrato anexo para processar.");
		}
		
		String identificacaoArquivo = anexoVO.asString("CHAVEARQUIVO");
        String caminhoRepositorio = "/home/mgeweb/Sistema/Anexos/AD_CONCILIACKONCILI/";        
        String novoCaminho = mudarExtensaoArquivo(caminhoRepositorio+identificacaoArquivo); 
        
        if (novoCaminho != null) {
            Workbook workbook = Workbook.getWorkbook(new File(novoCaminho));
            Sheet sheet = workbook.getSheet(0);
            int linhas = sheet.getRows();
            int colunas = sheet.getColumns();
            
            JapeWrapper konciliDAO = JapeFactory.dao("AD_REPASSESKONCILICAB");
            
            for(int i = 1; i < linhas; ++i) {
                
                Cell dataConciliacao = sheet.getCell(0, i);
                Cell nroConciliacao = sheet.getCell(1,i);
                
                if (!dataConciliacao.getContents().isEmpty() && !nroConciliacao.getContents().isEmpty()) {
                	
                	Collection<DynamicVO> konciliColVO = konciliDAO.find("NROCONCILIACAO = ?", nroConciliacao.getContents());
                	
                	if (konciliColVO == null) {
                		continue;
                	}
                	
                	for (DynamicVO konciliVO : konciliColVO) {
                		if (konciliVO.asTimestamp("DATABAIXACONC") != null) {
                			continue;
                		}
                		konciliDAO.prepareToUpdate(konciliVO).set("DATABAIXACONC", TimeUtils.toTimestamp(dataConciliacao.getContents())).update();                		
                	}
                }
            }

            contextoAcao.setMensagemRetorno("Arquivo processado com sucesso!");
            
        } else {
        	contextoAcao.mostraErro("Caminho do repostório não encontrado. Por favor, informe o caminho no parâmetro PATHIMPEXCEL na tela Preferências.");
        }
    }
	
	private String mudarExtensaoArquivo(String arquivo) {
    	File oldName = new File(arquivo);
    	File newName = new File(arquivo+".xls");
    	oldName.renameTo(newName);
    	
    	return arquivo+".xls";
    }

	private BigDecimal RetornaProximoLote() throws Exception {
        System.out.println("Entrei para retornar o próximo lote");
        JdbcWrapper jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT ISNULL(MAX(LOTE),0) + 1 AS LOTE FROM AD_IMPXLSDESC");
        
        ResultSet rs = sql.executeQuery();

        BigDecimal numeroLote;
        for(numeroLote = null; rs.next(); numeroLote = rs.getBigDecimal("LOTE")) {
        	
        }

        rs.close();
        System.out.println("Antes de retornar o próximo lote");
        return numeroLote;
    }

    private String RetornaCaminhoRepositorio() throws Exception {
        System.out.println("Entrei para retornar o caminho do repositório");
        JdbcWrapper jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT TEXTO FROM TSIPAR WHERE CHAVE = 'PATHIMPEXCEL'");
        ResultSet rs = sql.executeQuery();

        String caminho;
        for(caminho = null; rs.next(); caminho = rs.getString("TEXTO")) {
        }

        rs.close();
        System.out.println("Antes de retornar o caminho do repositório");
        return caminho;
    }

}
