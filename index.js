const fetch = require("node-fetch")
const _ = require("underscore")
const fs = require('fs');
const { Console } = require("console");


//elap 5
// const elasticURL = "https://i-o-optimized-deployment-b8eb6b.es.eu-west-1.aws.found.io:9243"
// const elasticAuth = "Basic ZWxhc3RpYzpXWm5xUmxDd2ptS2lRZURXSlRiSk01UEQ="

// elap 4
const elasticURL = "https://4563a979508a4f50a6f6a0f8332a3293.eu-west-1.aws.found.io:9243"
const elasticAuth = "Basic ZWxhc3RpYzpFRkUwNHhkQmFwNzFVY3Y5aldXaHRwSlY="

function unique(array){
  const result = [];

  for (let org of array){
    if(!result.includes(org)){
      result.push(org)
    }

  }
} 

const callingCode = "+1"

async function getOrganizationsArray(){

      ////////  GET ORGANIZATIONS COUNT ////////

      let countRes = await fetch(`${elasticURL}/organizations/_count`, {
        "method": "POST",
        "headers": {
          "Content-Type": "application/json",
          "Authorization": elasticAuth
        },
        "body": `{"query": {"bool": {"must": [{"match": {"keywords": "software"}},{"match": {"location_hq": "United States"}}]}}}`
      })
      let jsonCount = await countRes.json();
      let count = jsonCount.count

      ////////////////

      const organizations = [];
      var json;
      var hits;
/////// REQUEST FOR COUNT > 100000  ////
      if(count >= 10000){

        let options ={
          method: "POST",

          headers: {
            "Content-Type": "application/json",
            Authorization: elasticAuth
          },
          "body": `{"query": {"bool": {"must": [{"match": {"keywords": "software"}},{"match": {"location_hq": "United States"}}]}}}`
        }
          let response = await fetch(`${elasticURL}/organizations/_search?scroll=1m`, options)
          json = await response.json()
          hits = json.hits.hits
          hits.map(elem => organizations.push(elem._source.organization_name))
          let scrollId = json._scroll_id



        while (count > 10000){

        ///// GET NEXT SCROLL //////

          options.body = `{"scroll": "1m", "scroll_id": "${scrollId}"}`
          response = await fetch(`${elasticURL}/_search/scroll`, options)
          json = await response.json()
          crollId = json._scroll_id
          hits = json.hits.hits
          hits.map(elem => organizations.push(elem._source.organization_name))


        ///////////////////////////
          count -= 10000;
        }
        options.body = `{"scroll": "1m", "scroll_id": "${scrollId}"}`
          response = await fetch(`${elasticURL}/_search/scroll`, options)
          json = await response.json()
          hits = json.hits.hits
          hits.map(elem => organizations.push(elem._source.organization_name))
      }


      else{
        let options ={
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: elasticAuth
          },
          "body": `{"size": "${count}", "_source":["name"],"query":{"match":{"keywords":"software"}}}`
        }
        response = await fetch(`${elasticURL}/organizations/_search`, options)
        json = await response.json()
        hits = json.hits.hits
        hits.map(elem => console.log(elem._source))
        hits.map(elem => organizations.push(elem._source.organization_name))
      }
      return _.uniq(organizations)
}


async function loadCityToCSV(city) {

  console.log("fetch results from the elastic for", city)
  let uniqOrganizations = await getOrganizationsArray();
  console.log("Uniq organizations: ", uniqOrganizations.length)
  const mails = [];
  for(let i = 0; i <= uniqOrganizations.length; i++){
    try{

      let countRes = await fetch(`${elasticURL}/profiles/_search`, {
      "method": "POST",
      "headers": {
        "Content-Type": "application/json",
        "Authorization": elasticAuth
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
        let pos = hit.title
        let organization = hit.organization
        let country = hit.country
        let title = hit.title
        let org = hit.organization

        if(mails.includes(email1)){

        }else {
          mails.push(email1)
          fs.appendFileSync('audience.csv', `${email1},${email2},${email3},${phone1},${phone2},${phone3},${fn},${ln},${ct},${country},${title},${org}` + `\n`);
        }



      })

    }catch(e){
      console.log("Error: ", e)
    }


    console.log("Current org: ", i)
  }

}


if (!fs.existsSync('newAudience.csv')) {
  let fbHeader = 'email\temail\temail\tphone\tphone\tphone\tfn\tln\tposition\torganization\tct\tcountry\n'
  fs.appendFileSync('newAudience.csv', fbHeader);
}

loadCityToCSV("Palo Alto")
