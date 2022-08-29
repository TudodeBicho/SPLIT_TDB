package br.com.sattva.tdb.job;

import java.math.BigDecimal;

public class Split {
	public BigDecimal codEmp;
	public BigDecimal codProd;
	public BigDecimal qtdNeg = BigDecimal.ZERO;

	public Split(BigDecimal codEmp, BigDecimal codProd, BigDecimal qtdNeg) {
		this.codEmp = codEmp;
		this.codProd = codProd;
		this.qtdNeg = qtdNeg;
	}
	
	public Split() {
		
	}
	
	public void somaQtd(BigDecimal qtd) {
		this.qtdNeg = this.qtdNeg.add(qtd);
	}
	
	public String toString() {
		return "[Empresa: " + codEmp + ", Produto: " + codProd + ", Quantidade: " + qtdNeg +"]";
	}
}
