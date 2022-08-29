package br.com.sattva.tdb.job;

import java.math.BigDecimal;

public class Transferencia {
	public BigDecimal codProd;
	public BigDecimal qtdNeg;
	
	public Transferencia(BigDecimal codProd, BigDecimal saldo) {
		this.codProd = codProd;
		this.qtdNeg = saldo;
	}

}
