import express, { Request, Response } from 'express';
import cors from 'cors';
import multer from 'multer';
import Papa from 'papaparse';
import fs from 'fs';
import archiver from 'archiver';
import path from 'path';
import { createReadStream, createWriteStream, unlink } from 'fs';

const app = express();
app.use(cors());

const upload = multer({ dest: 'uploads/' });

interface MulterRequest extends Request {
  file: Express.Multer.File;
}

app.post('/uploadCSV', upload.single('file'), (req: any, res: any) => {
  if (!req.file) {
    return res.status(400).json({ message: 'No file uploaded' });
  }

  const filePath = req.file.path;
  const tempDir = 'temp';

  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
  }

  const maleFilePath = path.join(tempDir, 'male_data.csv');
  const femaleFilePath = path.join(tempDir, 'female_data.csv');
  const zipFilePath = path.join(tempDir, 'gender_data.zip');

  const maleStream = createWriteStream(maleFilePath);
  const femaleStream = createWriteStream(femaleFilePath);
  const fileStream = createReadStream(filePath, 'utf8');

  Papa.parse(fileStream, {
    header: true,
    skipEmptyLines: true,
    transform: (value) => value.toLowerCase().trim(),
    step: (row) => {
      const data: any = row.data;
      
      const gender = data.gender ? data.gender.toLowerCase().trim() : 'other';

      if (gender === 'male') {
        maleStream.write(Papa.unparse([data]));
      } else if (gender === 'female') {
        femaleStream.write(Papa.unparse([data]));
      }
    },
    complete: () => {
      // Close the CSV streams
      maleStream.end();
      femaleStream.end();

      // Create ZIP file after parsing is complete
      const archive = archiver('zip', { zlib: { level: 9 } });

      const output = createWriteStream(zipFilePath);
      output.on('close', () => {
        // Send the ZIP file and clean up
        res.download(zipFilePath, 'gender_data.zip', (err: any) => {
          // Clean up files after sending
          unlink(filePath, () => {});
          unlink(maleFilePath, () => {});
          unlink(femaleFilePath, () => {});
          unlink(zipFilePath, () => {});
        });
      });

      archive.pipe(output);
      archive.file(maleFilePath, { name: 'male_data.csv' });
      archive.file(femaleFilePath, { name: 'female_data.csv' });
      archive.finalize();
    }
  });
});

// Add an error handler
app.use((err: Error, req: Request, res: Response, _next: any) => {
  console.error('Error:', err);
  res.status(500).json({ message: 'Internal server error', error: err.message });
});

app.listen(8080, () => {
  console.log('Server started on port 8080');
});
