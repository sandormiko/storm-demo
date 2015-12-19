package hu.sm.storm.base.domain;

import java.io.Serializable;

@SuppressWarnings("serial")
public class KafkaMessage implements Serializable, Comparable<KafkaMessage> {

	private Integer id;
	private Integer random;
	public String data;
	private int sizeInBytes;

	public KafkaMessage(Integer id, Integer random, String charSequence, int aSizeInBytes) {
		super();
		this.id = id;
		this.random = random;
		this.data = charSequence;
		this.sizeInBytes = aSizeInBytes;
	}

	public Integer getId() {
		return id;
	}

	public Integer getRandom() {
		return random;
	}

	public String getData() {
		return data;
	}

	public String getTargetTopic() {
		return "random" + getRandom();
	}

	@Override
	public int compareTo(KafkaMessage other) {

		return this.id.compareTo(other.getId());
	}

	public int getSizeInBytes() {
		return sizeInBytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((random == null) ? 0 : random.hashCode());
		result = prime * result + sizeInBytes;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KafkaMessage other = (KafkaMessage) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (random == null) {
			if (other.random != null)
				return false;
		} else if (!random.equals(other.random))
			return false;
		if (sizeInBytes != other.sizeInBytes)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KafkaMessage [id=" + id + ", random=" + random + ", data=" + data + ", sizeInBytes=" + sizeInBytes
				+ "]";
	}

}
