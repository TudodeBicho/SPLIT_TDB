package br.com.sattva.tdb.utils;

import java.math.BigDecimal;

import br.com.sankhya.modelcore.comercial.ComercialUtils;
import br.com.sattva.tdb.job.Split;

public class TdbHelper {
	final static BigDecimal codLocal = new BigDecimal("1001"); 

	public static BigDecimal verificaSaldoEstoque(BigDecimal codEmp, BigDecimal codProd) throws Exception {
		ComercialUtils.ResultadoValidaEstoque resulEstoque;
		resulEstoque = ComercialUtils.validaEstoque(codEmp, codLocal, codProd, " ", null);
		return resulEstoque.getQtdEst();

	}

	public static void transfereSaldo6x1(BigDecimal codProd, BigDecimal saldo) {
		// TODO Auto-generated method stub		
	}

	public static void registraLogSplit(String string) {
		// TODO Auto-generated method stub
	}

	public static void geraLancamentosSplit(BigDecimal asBigDecimal, Split pedido) {
		// TODO Auto-generated method stub
	}

}
