from elasticsearch import Elasticsearch
import elasticsearch
import datetime
import json
import requests

#from flockos import chat, roster, users, channels
#from flockos import ActionConfig, Attachment, AttachmentButton, AttachmentDownload, Image, HtmlView, ImageView, Message, 

#SendAs,Views, WidgetView, PublicProfile, Channel



#DB Connection block

def get_ES_Conn(my_host,my_port):
     my_host=my_host
     my_port=my_port
     print("Going to connect ES")
     es = Elasticsearch([{'host': my_host , 'port':my_port }],timeout=1000)
     print("Connection to ES is Successfuly made")
     print(es)
     return es

es=get_ES_Conn('10.12.2.85','9200')

query7 = {
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "7",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(efdt)": {
			"max": {
				"field": "efdt"
			}
		}
	}
}

res=es.search(index='pd_activity',body=query7)
Mefdt=str(res['aggregations'][u'MAX(efdt)'][u'value_as_string'])

Mefdt_pediatric_associates=Mefdt[0:10:1]
print(Mefdt_pediatric_associates)

query10={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "10",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(efdt)": {
			"max": {
				"field": "efdt"
			}
		}
	}
}
res=es.search(index='pd_activity',body=query10)
Mefdt=str(res['aggregations'][u'MAX(efdt)'][u'value_as_string'])

Mefdt_cockerell=Mefdt[0:10:1]
print(Mefdt_cockerell)

query9={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "9",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(efdt)": {
			"max": {
				"field": "efdt"
			}
		}
	}
}

res=es.search(index='pd_activity',body=query9)
Mefdt=str(res['aggregations'][u'MAX(efdt)'][u'value_as_string'])

Mefdt_Leawood=Mefdt[0:10:1]
print(Mefdt_Leawood)

query6={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "6",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(rxfdt)": {
			"max": {
				"field": "rxfdt"
			}
		}
	}
}

res=es.search(index='pd_activity',body=query6)
Mrxfdt=str(res['aggregations'][u'MAX(rxfdt)'][u'value_as_string'])

Mrxfdt_pharmacy=Mrxfdt[0:10:1]
print(Mrxfdt_pharmacy)


query1={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "1",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(clsfdt)": {
			"max": {
				"field": "clsfdt"
			}
		}
	}
}

res=es.search(index='pd_activity',body=query1)
Mncmclsfdt=str(res['aggregations'][u'MAX(clsfdt)'][u'value_as_string'])

Mclsfdt_non_cmics_prof_claim=Mncmclsfdt[0:10:1]
print(Mclsfdt_non_cmics_prof_claim)

query8={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "8",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(clsfdt)": {
			"max": {
				"field": "clsfdt"
			}
		}
	}
}

res=es.search(index='pd_activity',body=query8)
Mcmclsfdt=str(res['aggregations'][u'MAX(clsfdt)'][u'value_as_string'])

Mclsfdt_cmics_prof_claim=Mcmclsfdt[0:10:1]
print(Mclsfdt_cmics_prof_claim)

query11={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "11",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(ofdt)": {
			"max": {
				"field": "ofdt"
			}
		}
	}
}

res=es.search(index='pd_activity',body=query11)
Mofdt=str(res['aggregations'][u'MAX(ofdt)'][u'value_as_string'])

Mofdt_labcorp=Mofdt[0:10:1]
print(Mofdt_labcorp)

query4={
	"from": 0,
	"size": 0,
	"query": {
		"bool": {
			"must": {
				"match": {
					"sid": {
						"query": "4",
						"type": "phrase"
					}
				}
			}
		}
	},
	"_source": {
		"includes": [
			"MAX"
		],
		"excludes": []
	},
	"aggregations": {
		"MAX(clsfdt)": {
			"max": {
				"field": "clsfdt"
			}
		}
	}
}


res=es.search(index='pd_activity',body=query4)
Medcclsfdt=str(res['aggregations'][u'MAX(clsfdt)'][u'value_as_string'])

Mclsfdt_med_claims=Medcclsfdt[0:10:1]
print(Mclsfdt_med_claims)



url="https://api.flock.com/hooks/sendMessage/b1a8ef66-d4b6-4d9c-beb2-932503db0139"


payload={
	"text": "Leawood records ingested  till date: "+ Mefdt_Leawood+"\n"+"cockrell records ingested till date:  "+Mefdt_cockerell+"\n"+"Pediatric associates records ingested till date : "+Mefdt_pediatric_associates+"\n"+ "Pharmacy Claims ingested till date: "+ Mrxfdt_pharmacy +"\n"+"Medical Claims ingested till date:  "+  Mclsfdt_med_claims+"\n"+"CMICS Professional claim ingested till date: "+ Mclsfdt_cmics_prof_claim+"\n"+"Non Cmics Professional claim ingested till date: "+ Mclsfdt_non_cmics_prof_claim+"\n"+"Lab corp data ingested till date:  "+ Mofdt_labcorp
}

payload = json.dumps(payload)
headers = {'Content-Type': 'application/json'}
response = requests.post(url, headers=headers, data=payload)
print( 'script end')







