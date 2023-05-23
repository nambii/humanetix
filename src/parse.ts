import stream from "stream";
import * as fs from "fs";
import { promisify } from "util";
import Pick from "stream-json/filters/Pick.js";
import StreamArray from "stream-json/streamers/StreamArray.js";

const pipeline = promisify(stream.pipeline);

const parseJson = async () => {
  const jsonParser = new StreamArray();
  let patients = [];
  let nurses = {};
  let doctors = [];

  jsonParser.on("readable", function () {
    const stream = this;
    let item;
    while ((item = stream.read())) {
      if (item.value.class === "Nurse") {
        if (!(`${item.value.ward_number}` in nurses)) {
          nurses[`${item.value.ward_number}`] = [];
        }
        nurses[`${item.value.ward_number}`].push(item.value);
      } else if (item.value.class === "Patient") {
        patients.push(item.value);
      } else {
        doctors.push(item.value);
      }
    }
  });

  jsonParser.on("end", function () {
    displayPatients(patients);
    displayNurses(nurses);
    displayDoctors(doctors);
  });

  await pipeline(
    await fs.createReadStream(
      new URL("./humanetix_data.json", import.meta.url)
    ),
    Pick.withParser({ filter: "" }),
    jsonParser
  );
};

const displayPatients = (patients: any) => {
  patients = patients.sort((p1, p2) => {
    if (
      `${p1.first_name} ${p1.family_name}` <
      `${p1.first_name} ${p2.family_name}`
    ) {
      return -1;
    }
  });
  console.log("Patients");
  patients.map((patient) =>
    console.log(` ${patient.first_name} ${patient.family_name}`)
  );
};

const displayNurses = (nurses: any) => {
  const items = Object.keys(nurses).map(function (key) {
    return [key, nurses[key]];
  });

  let sorted_wards = items.sort((w1: any, w2: any) => {
    return parseInt(w1) - parseInt(w2);
  });

  console.log("Nurses");
  for (let wIndex in sorted_wards) {
    console.log(` [Ward ${sorted_wards[wIndex][0]}]`);
    let sorted_nurses = sorted_wards[wIndex][1].sort((n1, n2) => {
      if (
        `${n1.first_name} ${n2.family_name}` <
        `${n1.first_name} ${n2.family_name}`
      ) {
        return -1;
      }
    });
    sorted_nurses.map((nurse) =>
      console.log(`  ${nurse.first_name} ${nurse.family_name}`)
    );
  }
};

const displayDoctors = (doctors: any) => {
  console.log("Doctors");
  doctors = doctors.sort((d1, d2) => {
    if (
      `${d1.first_name} ${d1.family_name}` <
      `${d2.first_name} ${d2.family_name}`
    ) {
      return -1;
    }
  });
  doctors.map((doctor) =>
    console.log(
      ` ${doctor.first_name} ${doctor.family_name}: ${doctor.pager_number}`
    )
  );
};
await parseJson();
