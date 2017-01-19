/**
 * @author Tobias Lindener
 */
package master2016;

public class hbaseApp {
	public static void main(String[] args) {
		int mode = 0;
		if (args.length > 0) {
			try {
				mode = Integer.parseInt(args[0]);
			} catch (NumberFormatException ex) {
				System.out.println("Unable to parse mode value. Please use only integer values");
			}
			if (args[1] == null) {
				System.out.println("No zookeeper provided");
			}
			System.out.println("Started hbaseApp with mode: " + args[0]);
			String zookeeper = args[1];
			TrendingHashtagsHbase hbaseApp = new TrendingHashtagsHbase(zookeeper);

			String startTS = null;
			String endTS = null;
			Integer number = null;
			String language = null;
			String outputFolder = null;

			switch (mode) {
			case 1:
				if (args.length != 7) {
					System.out.println(
							"Missing parameters: 1 cesvimaXXXX:2181 1450714465000 1450724465000 7 en /local/output/");
					System.exit(1);
				}
				startTS = args[2];
				endTS = args[3];
				number = Integer.parseInt(args[4]);
				language = args[5];
				outputFolder = args[6];
				hbaseApp.findNMostUsedWordsByLanguageAndTime(startTS, endTS, number, language, outputFolder);
				break;
			case 2:
				if (args.length != 7) {
					System.out.println(
							"Missing parameters: 2 cesvimaXXXX:2181 1450714465000 1450724465000 5 en,it,es /local/output/");
					System.exit(1);
				}
				startTS = args[2];
				endTS = args[3];
				number = Integer.parseInt(args[4]);
				language = args[5];
				outputFolder = args[6];
				hbaseApp.findListOfTopNMostUsedWordsByLanguageAndTime(startTS, endTS, number, language.split(","),
						outputFolder);
				break;
			case 3:
				if (args.length != 6) {
					System.out.println(
							"Missing parameters: 3 cesvimaXXXX:2181 1450714465000 1450724465000 10 /local/output/");
					System.exit(1);
				}
				startTS = args[2];
				endTS = args[3];
				number = Integer.parseInt(args[4]);
				outputFolder = args[5];
				hbaseApp.findTopNMostUsedWordsAndFrequencyByTime(startTS, endTS, number, outputFolder);
				break;
			case 4:
				if (args.length != 3) {
					System.out.println("Missing parameters: 4 cesvimaXXXX:2181 /local/data");
					System.exit(1);
				}
				String dataFolder = args[2];
				System.out.println("Executing loadDataInHbase");
				hbaseApp.loadDataInHbase(dataFolder);
				break;
			}

		} else {
			System.out.println("Missing arguments");
			System.exit(1);
		}

	}

}
