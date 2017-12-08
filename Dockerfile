FROM openjdk:8

RUN \
	apt-get update 

WORKDIR /generator

ADD run.sh  				/generator/run.sh
ADD src/main/resources/*  	/generator/resources/
ADD libs			  		/generator/libs/
ADD build/libs/*	  		/generator/libs/

ENV CP libs:resources

VOLUME [ "/generator/data" ]

EXPOSE 8111

ENTRYPOINT [ "sh", "/generator/run.sh" ]  
CMD [ "/generator/data" ]

