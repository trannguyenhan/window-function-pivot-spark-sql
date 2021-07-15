package part2;

import java.util.List;

public class Part2 {
	private String ParticipantID;
	private String Assessment;
	private String GeoTag;
	private String Qid_1;
	private String Qid_2;
	private String Qid_3;
	private List<String> Qids;
	
	public List<String> getQids() {
		return Qids;
	}

	public void setQids(List<String> qids) {
		Qids = qids;
	}

	public String getParticipantID() {
		return ParticipantID;
	}
	
	public void setParticipantID(String participantID) {
		ParticipantID = participantID;
	}
	
	public String getAssessment() {
		return Assessment;
	}
	
	public void setAssessment(String assessment) {
		Assessment = assessment;
	}
	
	public String getGeoTag() {
		return GeoTag;
	}
	
	public void setGeoTag(String geoTag) {
		GeoTag = geoTag;
	}
	
	public String getQid_1() {
		return Qid_1;
	}
	
	public void setQid_1(String qid_1) {
		Qid_1 = qid_1;
	}
	
	public String getQid_2() {
		return Qid_2;
	}
	
	public void setQid_2(String qid_2) {
		Qid_2 = qid_2;
	}
	
	public String getQid_3() {
		return Qid_3;
	}
	
	public void setQid_3(String qid_3) {
		Qid_3 = qid_3;
	}
	
	
}
