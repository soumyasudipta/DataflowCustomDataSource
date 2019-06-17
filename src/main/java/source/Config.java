package source;

public class Config {
	public static String[] websocketURI = {"wss://stream.binance.com:9443/ws/bnbbtc@ticker",
			"wss://stream.binance.com:9443/ws/ethbtc@ticker",
			"wss://stream.binance.com:9443/ws/neobtc@ticker",
			"wss://stream.binance.com:9443/ws/ltcbtc@ticker",
			"wss://stream.binance.com:9443/ws/dashbtc@ticker",
			"wss://stream.binance.com:9443/ws/zecbtc@ticker"};
	
	public static String[] getURI() {
		return websocketURI;
	}
}
