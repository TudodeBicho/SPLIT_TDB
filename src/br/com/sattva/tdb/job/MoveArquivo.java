package br.com.sattva.tdb.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class MoveArquivo implements ScheduledAction {
	public BufferedReader br;
	
	@Override
	public void onTime(ScheduledActionContext arg0) {
		try {
			
			String caminhoOrigemItauEconic = "/home/mgeweb/Apus/ECONIC/REMESSA/ITAU/Remessa/";
			String caminhoOrigemItauTDB = "/home/mgeweb/Apus/TDB/REMESSA/ITAU/Remessa/";
			String caminhoDestino = "/home/mgeweb/RemovidosApus/";
			
			File newArchaive = new File(caminhoDestino);
						
			File oldArchaiveEconic = new File(caminhoOrigemItauEconic);
			
	    	for (String arquivo : oldArchaiveEconic.list()) {
	    		
	    		File arquivoCorrente = new File(oldArchaiveEconic + "/" + arquivo);
	    		if(arquivoCorrente.isDirectory()) {
	    			continue;
	    		}
	    		
	    		String linha = "";
	    		boolean moveArquivo = true;
	    		
	    		br = new BufferedReader(new InputStreamReader(new FileInputStream(oldArchaiveEconic + "/" + arquivo), "LATIN1"));
	    		while ((linha = br.readLine()) != null) {
	    			String registroDetalhe = linha.substring(6, 7);
	    			if (registroDetalhe.equals("1")) {
	    				moveArquivo = false;
	    			}
	    		}
	    		
	    		br.close();
	    		
	    		if (moveArquivo) {
	    			File antigoCaminho = new File(caminhoOrigemItauEconic + arquivo);
	    			File novoCaminho = new File(caminhoDestino + arquivo);
	    			antigoCaminho.renameTo(novoCaminho);
	    		}
	    	}
	    	
	    	File oldArchaiveTDB = new File(caminhoOrigemItauTDB);
	    	for (String arquivo : oldArchaiveTDB.list()) {
	    		
	    		File arquivoCorrente = new File(oldArchaiveTDB + "/" + arquivo);
	    		if(arquivoCorrente.isDirectory()) {
	    			continue;
	    		}
	    			    		
	    		String linha = "";
	    		boolean moveArquivo = true;
	    		
	    		br = new BufferedReader(new InputStreamReader(new FileInputStream(oldArchaiveTDB + "/" + arquivo), "LATIN1"));
	    		while ((linha = br.readLine()) != null) {
	    			String registroDetalhe = linha.substring(6, 7);
	    			if (registroDetalhe.equals("1")) {
	    				moveArquivo = false;
	    			}
	    		}
	    		
	    		br.close();
	    		
	    		if (moveArquivo) {
	    			File antigoCaminho = new File(caminhoOrigemItauTDB + arquivo);
	    			File novoCaminho = new File(caminhoDestino + arquivo);
	    			antigoCaminho.renameTo(novoCaminho);
	    		}
	    	}
	    	
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
