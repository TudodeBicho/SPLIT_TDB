package br.com.sattva.tdb.job;

import java.math.BigDecimal;

public class Split {
	public BigDecimal codEmp;
	public BigDecimal codProd;
	public BigDecimal qtdNeg;
	
	public String toString() {
		return "[Empresa: " + codEmp + ", Produto: " + codProd + ", Quantidade: " + qtdNeg +"]";
	}
}
