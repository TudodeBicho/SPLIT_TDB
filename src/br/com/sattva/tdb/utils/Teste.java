package br.com.sattva.tdb.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;

import br.com.sattva.tdb.job.Split;

public class Teste {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Collection<Split> quebraPedido = new ArrayList();
		Collection<Split> itensEmpresa1 = new ArrayList();
		Collection<Split> itensEmpresa5 = new ArrayList();
		Collection<Split> itensEmpresa1Agrupado = new ArrayList();
		Collection<Split> itensEmpresa1Agrupado2 = new ArrayList();
		Collection<Split> itensEmpresa5Agrupado = new ArrayList();
		
		Split a = new Split(new BigDecimal("5"), new BigDecimal("107199"), new BigDecimal("30"));
		Split b = new Split(new BigDecimal("1"), new BigDecimal("107199"), new BigDecimal("1150"));
		Split c = new Split(new BigDecimal("1"), new BigDecimal("107199"), new BigDecimal("1"));
		Split d = new Split(new BigDecimal("1"), new BigDecimal("107199"), new BigDecimal("9"));
		Split z = new Split(new BigDecimal("1"), new BigDecimal("555"), new BigDecimal("9"));
		Split e = new Split(new BigDecimal("5"), new BigDecimal("123"), new BigDecimal("2"));
		Split f = new Split(new BigDecimal("5"), new BigDecimal("123"), new BigDecimal("3"));
		Split g = new Split(new BigDecimal("5"), new BigDecimal("124"), new BigDecimal("3"));
		
		quebraPedido.add(a); quebraPedido.add(b); quebraPedido.add(c); quebraPedido.add(d);
		quebraPedido.add(e); quebraPedido.add(f); quebraPedido.add(g); quebraPedido.add(z);
		
		for (Split teste : quebraPedido) {
			System.out.println(teste.toString());
			if (teste.codEmp.intValue() == 1) {
				itensEmpresa1.add(teste);
			} else {
				itensEmpresa5.add(teste);
			}
		}
		
		System.out.println("\nOrganizando...\n");
		
		for(Split emp1 : itensEmpresa1) {
			System.out.println(emp1.toString());
		}
		
		for(Split emp5 : itensEmpresa5) {
			System.out.println(emp5.toString());
		}
		
		System.out.println("\nAgrupando...\n");
		
		ArrayList<BigDecimal> produtos = new ArrayList<BigDecimal>();
		
		for(Split emp1 : itensEmpresa1) {
			produtos.add(emp1.codProd);
		}
		
		ArrayList<BigDecimal> produtosNew =TdbHelper.removeDuplicates(produtos);
		System.out.println(produtosNew.toString());
		
		for (BigDecimal produto : produtosNew) {
			Split s = new Split(new BigDecimal("1"), produto, BigDecimal.ZERO);
			itensEmpresa1Agrupado.add(s);			
		}
		
		for (Split sp : itensEmpresa1Agrupado) {
			BigDecimal qtdSomada = BigDecimal.ZERO;
			for (Split ie1 : itensEmpresa1) {
				if(ie1.codProd.intValue() == sp.codProd.intValue()) {
					qtdSomada = qtdSomada.add(ie1.qtdNeg);
				}
			}
			
			itensEmpresa1Agrupado2.add(new Split(new BigDecimal("1"), sp.codProd, qtdSomada));
		}
		
		System.out.println("\n Agora vai \n");
		
		for (Split aa : itensEmpresa1Agrupado2) {
			System.out.println(aa.toString());
		}
		
		
		
	}

}
