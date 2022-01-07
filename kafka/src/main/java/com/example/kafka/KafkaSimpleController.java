package com.example.kafka;

import com.google.gson.Gson;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaSimpleController {

    private KafkaTemplate<String, String> kafkaTemplate;
    private Gson jsonConvertor;

	@Autowired
	public KafkaSimpleController(KafkaTemplate<String, String> kafkaTemplate, Gson jsonConvertor){
		this.kafkaTemplate = kafkaTemplate;
        this.jsonConvertor = jsonConvertor;
	}

    @PostMapping
    public void post(@RequestBody SimpleModel simpleModel){
        kafkaTemplate.send("myTopic", jsonConvertor.toJson(simpleModel));
    }

    @KafkaListener(topics="myTopic")
    public void getFromKafka(String simpleModel){
        System.out.println(simpleModel);
        SimpleModel simpleModel1 = (SimpleModel) jsonConvertor.fromJson(simpleModel, SimpleModel.class);
        System.out.print(simpleModel1.toString());
    }
}