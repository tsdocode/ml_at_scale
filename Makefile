dashboard:
	streamlit run dashboard.py
streaming:
	spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 streaming.py