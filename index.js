const fetch = require("node-fetch")
const _ = require("underscore")
const fs = require('fs');
const { Console } = require("console");

function unique(array){
  const result = [];

  for (let org of array){
    if(!result.includes(org)){
      result.push(org)
    }
  }

  return result;

}

async function getOrganizationsArray(){
      ////////  GET ORGANIZATIONS COUNT ////////

      let countRes = await fetch("https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243/organizations/_count", {
        "method": "POST",
        "headers": {
          "Content-Type": "application/json",
          "Authorization": "Basic ZWxhc3RpYzpXWm5xUmxDd2ptS2lRZURXSlRiSk01UEQ="
        },
        "body": `{"query":{"match":{"keywords":"software"}}}`
      })
      let jsonCount = await countRes.json();
      let count = jsonCount.count
  
      ////////////////
     
      const organizations = [];
      var json;
      var hits;
/////// REQUEST FOR COUNT > 100000  ////      
      if(count >= 10000){
      ///// INIT SCROLL REQUEST ////
        let options ={
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: "Basic ZWxhc3RpYzpXWm5xUmxDd2ptS2lRZURXSlRiSk01UEQ="
          },
          "body": `{"size": "10000", "_source":["name"],"query":{"match":{"keywords":"software"}}}`
        }
          let response = await fetch("https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243/organizations/_search?scroll=1m", options)
          json = await response.json()
          hits = json.hits.hits
          hits.map(elem => organizations.push(elem._source.name))
          let scrollId = json._scroll_id

      ///////////////////////////////

        while (count > 10000){
        ///// GET NEXT SCROLL //////
        
          options.body = `{"scroll": "1m", "scroll_id": "${scrollId}"}`
          response = await fetch("https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243/_search/scroll", options)
          json = await response.json()
          crollId = json._scroll_id
          hits = json.hits.hits
          hits.map(elem => organizations.push(elem._source.name))

        ///////////////////////////  
          count -= 10000;
        }
        options.body = `{"scroll": "1m", "scroll_id": "${scrollId}"}`
          response = await fetch("https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243/_search/scroll", options)
          json = await response.json()
          hits = json.hits.hits
          hits.map(elem => organizations.push(elem._source.name))
      }
////////////////////////

      else{
        let options ={
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: "Basic ZWxhc3RpYzpXWm5xUmxDd2ptS2lRZURXSlRiSk01UEQ="
          },
          "body": `{"size": "${count}", "_source":["name"],"query":{"match":{"keywords":"software"}}}`
        }
        response = await fetch("https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243/organizations/_search", options)
        json = await response.json()
        hits = json.hits.hits
        hits.map(elem => organizations.push(elem._source.name))
      }
      return unique(organizations)
}


async function loadCityToCSV(city) {

  console.log("fetch results from the elastic for", city)
  let uniqOrganizations = await getOrganizationsArray();
  console.log(uniqOrganizations.length)
  const mails = [];
  for(let i = 0; i <= uniqOrganizations.length; i++){
    try{

      let countRes = await fetch("https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243/profiles/_search", {
      "method": "POST",
      "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic ZWxhc3RpYzpXWm5xUmxDd2ptS2lRZURXSlRiSk01UEQ="
      },
      "body": `{"query\":{"bool":{"must":[{"terms":{"title":["cmo","cto\","ceo","cio","product manager","program manager","it director","project manager","founder","cofounder"]}},{"match_phrase":{"organization":"${uniqOrganizations[i]}"}}]}}}`
  
      })
      let jsonCount = await countRes.json();
      let hits = jsonCount.hits.hits
      
      hits.map(_hit => {
        let hit = _hit._source;
        if (!Array.isArray(hit.email)){
          hit.email = [hit.email]
        }
        let emails = _.union([], hit.email)
        let email1 = emails.shift() || ''
        let email2 = emails.shift() || ''
        let email3 = emails.shift() || ''

        if (!Array.isArray(hit.phone)){
          hit.phone = [hit.phone]
        }
        let phones = _.union([], hit.phone)
        let phone1 = phones.shift() || ''
        let phone2 = phones.shift() || ''
        let phone3 = phones.shift() || ''

        let fn = hit.name.split(' ').shift()
        let ln = hit.name.replace(',', '').split(' ').slice(1).join(' ')
        let ct = hit.city
        let country = hit.country
        let value = _hit._score

        if(mails.includes(email1)){

        }else if(phone1){
          mails.push(email1)
          fs.appendFileSync('audience.csv', `${email1},${email2},${email3},${phone1},${phone2},${phone3},${fn},${ln},${ct},${country},${value}` + `\n`);
        }

        
        
      })

    }catch(e){
      console.log("Error: ", e)
    }
    
    
    console.log("Current org: ", i)
  }
  
}


if (!fs.existsSync('audience.csv')) {
  let fbHeader = 'email,email,email,phone,phone,phone,fn,ln,ct,st,country,value\n'
  fs.appendFileSync('audience.csv', fbHeader);
}

loadCityToCSV("Palo Alto")
