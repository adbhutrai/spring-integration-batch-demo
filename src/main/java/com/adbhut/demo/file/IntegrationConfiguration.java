package com.adbhut.demo.file;

import java.io.File;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.jpa.Jpa;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.jpa.support.PersistMode;
import org.springframework.messaging.MessageChannel;

import com.adbhut.demo.file.batch.AccessService;
import com.adbhut.demo.file.batch.AccessServiceImpl;
import com.adbhut.demo.file.batch.ArchiveProductImportFileTasklet;
import com.adbhut.demo.file.batch.JobCompletionNotificationListener;
import com.adbhut.demo.file.batch.Person;
import com.adbhut.demo.file.batch.PersonItemProcessor;

@Configuration
public class IntegrationConfiguration {
    @Autowired
    private EntityManagerFactory entityManagerFactory;
    
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    public DataSource dataSource;

    @Autowired
    private JobCompletionNotificationListener listener;

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("file_name");
        fileMessageToJobRequest.setJob(personJob());
        return fileMessageToJobRequest;
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(new SyncTaskExecutor());
        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(simpleJobLauncher);
        return jobLaunchingGateway;
    }

    @Bean
    public IntegrationFlow integrationFlow(
            @Value("${input.directory:c:/Users/adbhu/Desktop/csv}") String inputDirectory,
            JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlows.from(Files.inboundAdapter(new File(inputDirectory))
                .preventDuplicates(true)
                .patternFilter("*.csv"),
                c -> c.poller(Pollers.fixedRate(1000, 2000)
                        .maxMessagesPerPoll(1)))
                .handle(fileMessageToJobRequest())
                .handle(jobLaunchingGateway)
                .handle(loggingHandler())
                .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "stepExecutionsChannel")
    public LoggingHandler loggingHandler() {
        LoggingHandler adapter = new LoggingHandler(LoggingHandler.Level.WARN);
        adapter.setLoggerName("TEST_LOGGER");
        adapter.setLogExpressionString("headers.id + ': ' + payload");
        return adapter;
    }

    @MessagingGateway(name = "notificationExecutionsListener", defaultRequestChannel = "stepExecutionsChannel")
    public interface NotificationExecutionListener extends StepExecutionListener {
    }
    
    @Bean
    public IntegrationFlow pollingAdapterFlow() {
        return IntegrationFlows
            .from(Jpa.inboundAdapter(this.entityManagerFactory)
                        .jpaQuery("select p from PersonEnitity p where p.accepted = 'N'"),
                e -> e.poller(p -> p.fixedDelay(1000)))
            .split()
            //.channel(c -> c.queue("pollingResults"))
            .handle("accessService", "process")
            .handle(Jpa.outboundAdapter(this.entityManagerFactory)
                    .flush(true)
                    .flushSize(50)
                    .persistMode(PersistMode.MERGE),
                    e -> e.transactional())
            .get();
    }
    
    
     @Bean
    public AccessService accessService() {
        return new AccessServiceImpl();
    }
    
    
    
   
    @Bean
    Job personJob() {
        return jobBuilderFactory.get("personJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(sampleReader(null))
                .processor(processor())
                .writer(writer())
                .build();
    }

   /* @JobScope
    @Bean
    public Step step(@Value("#{jobParameters[file_name]}") String resource) {
        return stepBuilderFactory.get("nextStep")
                .tasklet(moveFileTasklet(resource))
                .build();
    }
*/
    @Bean
    @StepScope
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Person> sampleReader(@Value("#{jobParameters[file_name]}") String resource) {
        System.out.println("------------------Resource : " + resource + "----------------------------------");
        FlatFileItemReader<Person> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setLinesToSkip(1);
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] { "firstName", "lastName" });
        lineMapper.setLineTokenizer(lineTokenizer);
        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Person.class);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setResource(new FileSystemResource(resource));
        return flatFileItemReader;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(this.dataSource);
        itemWriter.setSql("INSERT INTO people (first_name, last_name, accepted) VALUES (:firstName, :lastName, :accepted)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    @Bean
    @StepScope
    Tasklet moveFileTasklet(String resource) {
        ArchiveProductImportFileTasklet moveFileTasklet = new ArchiveProductImportFileTasklet();
        moveFileTasklet.setInputFile(resource);
        return moveFileTasklet;
    }
}
