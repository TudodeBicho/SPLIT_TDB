package br.com.sattva.tdb.job;

import java.math.BigDecimal;

public class Split {
	public BigDecimal codEmp;
	public BigDecimal codProd;
	public BigDecimal qtdNeg = BigDecimal.ZERO;
	public BigDecimal vlrUnit;

	public Split(BigDecimal codEmp, BigDecimal codProd, BigDecimal qtdNeg, BigDecimal vlrUnit) {
		this.codEmp = codEmp;
		this.codProd = codProd;
		this.qtdNeg = qtdNeg;
		this.vlrUnit = vlrUnit;
	}
	
	public Split() {
		
	}
	
	public void somaQtd(BigDecimal qtd) {
		this.qtdNeg = this.qtdNeg.add(qtd);
	}
	
	public String toString() {
		return "[Empresa: " + codEmp + ", Produto: " + codProd + ", Quantidade: " + qtdNeg + ", Valor.Unit: " + vlrUnit +"]";
	}
}
