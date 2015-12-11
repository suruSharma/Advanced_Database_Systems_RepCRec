package nyu.edu.adb.recpec;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Rahul Desai, Suruchi Sharma
 * Class that hold all information for a variable
 *
 */
public class Variable {
	private int ID;
	private int value;
	private boolean copied;
	private List<VariableHistory> historicalData;
	private boolean availableForRead;
	private int currValue;
	
	public Variable(int id){
		this.ID=id;
		this.value=id*10;
		historicalData = new ArrayList<VariableHistory>();
		VariableHistory initial = new VariableHistory(0,this.value);
		historicalData.add(initial);
		this.availableForRead=true;
		if(id%2==0){
			this.copied=true;
		}else{
			this.copied=false;
		}
		this.setCurrValue(this.getValue());
	}
	
	public int getCurrValue() {
		return currValue;
	}
	
	public void setCurrValue(int currValue) {
		this.currValue = currValue;
	}
	
	public boolean getAvailableForRead(){
		return this.availableForRead;
	}
	
	public void setAvailableForRead(boolean val){
		this.availableForRead=val;
	}
	
	public boolean isCopied() {
		return copied;
	}
	
	public void setCopied(boolean copied) {
		this.copied = copied;
	}
	
	public int getID(){
		return this.ID;
	}
	
	public int getValue(){
		return this.value;
	}
	
	public void setValue(int v){
		this.value=v;
	}
	
	public List<VariableHistory> getHistoricalData(){
		return this.historicalData;
	}
	
	public void addToHistoricalData(int time, int value){
		VariableHistory newData= new VariableHistory(time,value);
		this.historicalData.add(newData);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof Variable)) {
			return false;
		}
		Variable data = (Variable) o;
		return this.getID() == data.getID();
	}
	
	@Override
	public String toString(){
		return "x"+this.ID+" "+this.value+"\n";
	}
	
	/**
	 * Enhanced toString
	 * @return
	 */
	public String stringRepresentationOfHistoricalData(){
		StringBuilder answer = new StringBuilder();
		for(int i = 0 ; i < this.historicalData.size(); i++){
			answer.append("|");
			answer.append(historicalData.get(i).toString());
			answer.append("|");
		}
		answer.append("\n");
		return answer.toString();
	}
}