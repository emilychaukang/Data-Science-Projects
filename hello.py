#Import prefect
from prefect import flow,task

@task
def create_message():
	msg = 'Hello from Task'
	return(msg)

# Sub flow
@flow
def something_else():
	result = 10
	return (result) 
	

@flow
def hello_world():
	sub_flow_message = something_else()
	task_message = create_message()
	new_message = task_message +str(sub_flow_message)
	print(new_message)
	
if __name__ == "__main__" :
	hello_world()

