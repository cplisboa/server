import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class Logger {
	
	private String fileName = "SERVER-LOG-";
	private static final boolean PRINT_CONSOLE = true;
	private BufferedWriter escritor;
	private FileWriter fis;

	public Logger(String code, Date data) {
		fileName = code + "-" + data.getDate() + "-" + (data.getMonth()+1) + "-" + (data.getYear()+1900) + " " + data.getHours() + "-" +data.getMinutes();
		try {
			fis = new FileWriter(fileName+".txt");
			escritor = new BufferedWriter(fis);
		}catch(Exception e){
			System.out.println("Erro abrindo arquivo para escrita. "+fileName);
			e.printStackTrace();
		}	
	}
	/**
	 * Escreve no BufferedWriter
	 * @param log Linha para ser logada
	 */
	public void log(String log){
		try {
			escritor.write(log+"\n");
			if(PRINT_CONSOLE)
				System.out.println(log+"\n");
		} catch (IOException e) {
			System.out.println("Erro logando em arquivo");
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			//fis.close();
			escritor.close();
		} catch (IOException e) {
			System.out.println("Erro fechando arquivo de log");
			e.printStackTrace();
		}
	}

}
