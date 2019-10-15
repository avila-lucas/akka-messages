```
Conflict is what creates drama. The more conflict actors find, the more interesting the performance.
- Michael Shurtleff 
```
<div align="center">
    <img src="http://sheepfilms.co.uk/wp-content/uploads/2015/12/postbox_eyes_letters_small.gif" alt="Made by someone at giphy" width="200">
    <br>
    <h1>Akka Messages</h1>
    <sub>Built with ❤︎ by developers @UTN</sub>
</div>

---

## Purpose

Explore concurrency models. Akka is one of the main choices whenever you want to enter the concurrency world. Or at least that's what people say...let's find out how hard it is to deal with this :)

---

## Setup

### Scala + SBT
First download Scala + SBT from here: https://www.scala-lang.org/download/

### Akka 
Then Download this repo and let the IDE import all things. This might take a while...you can force the update from the console also with:
`sbt reload clean compile`
If you're interested in what does each command go to here: https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html

### Start this thing! 
When you have all things ready just use your favorite IDE and run the main class or execute `sbt run` at the root directory :)

## Endpoints
Up to this date we have the following endpoints: 
* Queues 

### :stars: Queues
```scala
[GET] /queues
```
Here you'll see all available queues' names.

```scala
[POST] /queues
```
To create queues.
```json
{
  "name": "cola_1",
  "producers": 1,
  "workers": 3,
  "jobInterval": 1,
  "spreadType": "pubsub" // Any other type will default to RoundRobin
}
``` 

```scala
[GET] /queues/:id
```
Extra info about queues? [WIP]